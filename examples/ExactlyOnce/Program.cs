// Copyright 2022 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;


namespace Confluent.Kafka.Examples.ExactlyOnce
{
    public class Program
    {
        private static ILogger logger = Utils.LoggerFactory.CreateLogger<Program>();

        public static async Task Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationTokenSource completedCancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (_, e) =>
            {
                cts.Cancel();
                completedCancellationTokenSource.Token.WaitHandle.WaitOne();
            };

            if (args.Length != 2)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  create_words <transactional-id>");
                Console.WriteLine("  reverse_words <transactional-id>");
                completedCancellationTokenSource.Cancel();
                return;
            }

            var command = args[0];
            var transactionalId = args[1];

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("./appsettings.json")
                .Build();

            var adminClientConfig = configuration.GetSection("AdminClient").Get<AdminClientConfig>();
            using var adminClient = new AdminClientBuilder(adminClientConfig).Build();

            switch (command)
            {
                case "create_words":
                    await WordCreator.Execute(configuration, transactionalId, adminClient, cts);
                    break;
                case "reverse_words":
                    await WordReverser.Execute(configuration, transactionalId, adminClient, cts);
                    break;
                default:
                    logger.LogError("Unknown command");
                    break;
            }

            completedCancellationTokenSource.Cancel();
        }
    }

    /// <summary>
    ///     This transactional producer allows to use a producer an consumer for transactions,
    ///     by recreating the producer when a fatal error happens, sending offsets to transaction
    ///     and handling aborts and retries correctly.
    /// </summary>
    public class TransactionalProducer<K, V> : IDisposable
    {
        private static ILogger logger = Utils.LoggerFactory.CreateLogger<TransactionalProducer<K, V>>();

        // TODO: This should not be here!! A thing called consumer should not be a mamber of a thing called a consumer.
        // This should be refactored to be a "Processor".
        public IConsumer<K, V> Consumer { get; set; }

        private IProducer<K, V> producer { get; set; }

        public string OutputTopic { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig ProducerConfig { get; set; }

        public CancellationTokenSource CancellationTokenSource { get; set; }

        public delegate bool RetryCondition(bool isFatal, bool txnRequiresAbort, bool isRetriable);
    
        public static void ThrowIfNotRetriable(string operation, KafkaException e, TimeSpan retryInterval)
        {
            var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var isRetriable = e is KafkaRetriableException;
            var isFatal = e.Error.IsFatal;
            logger.LogError($"{operation} Kafka Exception caught: '{e.Message}', IsFatal: {isFatal}, TxnRequiresAbort: {txnRequiresAbort}, IsRetriable: {isRetriable}");
            if (isFatal || txnRequiresAbort || !isRetriable)
            {
                throw e;
            }
            Thread.Sleep(retryInterval);
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds,
        ///     call <paramref name="onKafkaException"/> if a <see cref="Confluent.Kafka.KafkaException" /> occurs.
        /// </summary>
        private void Retry(string operation, Action action, Action<string, KafkaException, TimeSpan> onKafkaException = null)
        {
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    action();
                    break;
                }
                catch (KafkaException e)
                {
                    if (onKafkaException != null)
                    {
                        onKafkaException(operation, e, RetryInterval);
                    }
                }
            }
        }

        /// <summary>
        ///     Commits transaction's produced and consumed messages.
        ///     Aborts transaction if an abortable exception is thrown,
        ///     recreates the producer if a fatal exception is thrown or
        ///     retries each operation that is throwing a retriable exception.
        /// </summary>
        public void CommitKafkaTransaction(List<Message<K, V>> messages)
        {
            var recreateProducer = true;

            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (recreateProducer)
                    {
                        logger.LogInformation("(Re)creating producer");
                        if (producer != null) producer.Dispose();
                        producer = new ProducerBuilder<K, V>(ProducerConfig).Build();
                        producer.InitTransactions(LocalTransactionOperationTimeout);
                        recreateProducer = false;
                    }
                    
                    logger.LogInformation("Calling BeginTransaction.");
                    producer.BeginTransaction();

                    logger.LogInformation($"Producing {messages.Count} messages.");
                    foreach (var message in messages)
                    {
                        Retry("Produce", () => producer.Produce(OutputTopic, message), ThrowIfNotRetriable);
                    }

                    logger.LogInformation($"Producing message completed");

                    if (Consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            logger.LogInformation("Calling SendOffsetsToTransaction");
                            producer.SendOffsetsToTransaction(
                                Consumer.Assignment.Select(a => new TopicPartitionOffset(a, Consumer.Position(a))),
                                Consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, ThrowIfNotRetriable);
                        logger.LogInformation($"SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        logger.LogInformation("calling CommitTransaction");
                        producer.CommitTransaction();
                    }, ThrowIfNotRetriable);

                    logger.LogInformation($"CommitTransaction completed");
                    break;
                }
                catch (KafkaException e)
                {
                    logger.LogError($"Kafka Exception caught, aborting transaction, trying again in {RetryInterval.TotalSeconds} seconds: '{e.Message}'");
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        recreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            logger.LogInformation("calling AbortTransaction");
                            producer.AbortTransaction(LocalTransactionOperationTimeout);
                        }, (operation, eInner, retryInterval) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            logger.LogError($"AbortTransaction Kafka Exception caught, trying again in {RetryInterval.TotalSeconds} seconds: '{eInner.Message}', IsFatal: {IsFatalInner}, TxnRequiresAbort: {TxnRequiresAbortErrorInner}, IsRetriable: {IsRetriableInner}");
                            Thread.Sleep(RetryInterval);
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    recreateProducer = true;
                                }
                                // Propagate abort to consumer and application
                                throw e;
                            }
                        });
                        // Propagate abort to consumer and application
                        throw;
                    }
                    Thread.Sleep(RetryInterval);
                }
                catch
                {
                    logger.LogError("Caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            if (producer != null) producer.Dispose();
        }
    }

    public class WordCreatorConfig
    {
        public TopicSpecification OutputTopic { get; set; }

        public TimeSpan ProduceRate { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }
    }

    public static class WordCreator
    {
        private static ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordCreator));

        private static readonly Random random = new Random();

        private static string CreateWord()
        {
            int start = (int)'a';
            int end = (int)'z';
            int length = random.Next(5, 10);
            StringBuilder sb = new StringBuilder(10);
            char nextChar = (char)random.Next(start, end + 1);
            for (int i = 0; i < length; i++)
            {
                sb.Append(nextChar);
                nextChar = (char)random.Next(start, end + 1);
            }
            return sb.ToString();
        }

        public static async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            var config = configuration.GetSection("WordCreator").Get<WordCreatorConfig>();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            List<Message<Null, string>> createdWords = new List<Message<Null, string>>();

            var transactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = config.OutputTopic.Name,
                RetryInterval = config.RetryInterval,
                LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                ProducerConfig = config.Producer,
                CancellationTokenSource = cts
            };
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    if (!createdWords.Any())
                    {
                        createdWords.Add(new Message<Null, string> { Value = CreateWord() });
                    }
                    transactionalProducer.CommitKafkaTransaction(createdWords);
                    createdWords.Clear();
                    logger.LogInformation($"Produced one word");
                }
                catch (KafkaException)
                {
                    // Retry the same word
                }
                Thread.Sleep(config.ProduceRate);
            }

            transactionalProducer.Dispose();
            logger.LogInformation($"Process finished");
        }
    }


    public class WordReverserConfig
    {
        public TopicSpecification InputTopic { get; set; }

        public TopicSpecification OutputTopic { get; set; }

        public int CommitMaxMessages { get; set; }

        public TimeSpan CommitPeriod { get; set; }

        public TimeSpan CommitTimeout { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }

        public ConsumerConfig Consumer { get; set; }
    }

    /// <summary>
    ///     Reads words from a topic, reverse them, and send them to a different topic with
    ///     exactly once semantics.
    /// </summary>
    public static class WordReverser
    {
        private static ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordReverser));

        private static TransactionalProducer<Null, string> transactionalProducer;

        private static IConsumer<Null, string> consumer;

        private static List<Message<Null, string>> reversedWords = new List<Message<Null, string>>();

        private static DateTime? lastCommitTime = null;

        /// <summary>
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private static void RewindConsumer(TimeSpan timeout)
        {
            foreach (var committedOffset in consumer.Committed(consumer.Assignment, timeout))
            {
                var position = consumer.Position(committedOffset.TopicPartition);
                position = position < 0 ? Offset.Beginning : position;
                consumer.Seek(new TopicPartitionOffset(committedOffset.TopicPartition, position));
            }
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds.
        /// </summary>
        private static void Retry(Action action, TimeSpan retryInterval, CancellationTokenSource cts)
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    action();
                    break;
                }
                catch (KafkaException)
                {
                    Thread.Sleep(retryInterval);
                }
                catch (Exception e)
                {
                    logger.LogError($"Unexpected exception: {e}");
                    throw;
                }
            }
        }

        private static string Reverse(string original)
        {
            char[] originalChars = original.ToCharArray();
            Array.Reverse(originalChars);
            return new string(originalChars);
        }

        /// <summary>
        ///     Aborts the transaction by clearing words and resetting the offsets to
        ///     the committed ones, processing the messages again and sending to the next transaction.
        ///
        ///     Previously sent and aborted messages won't be read by a consumer with read_committed
        ///     isolation level.
        ///
        ///     If there's and application transaction involved too, this method should abort that
        ///     transaction too, otherwise any program failure can lead to a half-committed state.
        /// </summary>
        private static void AbortTransaction(TimeSpan commitTimeout, TimeSpan retryInterval, CancellationTokenSource cts)
        {
            reversedWords.Clear();
            Retry(() => RewindConsumer(commitTimeout), retryInterval, cts);
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This method should commit any application transaction too.
        /// </summary>
        private static void CommitMaybe()
        {
            if (reversedWords.Any())
            {
                transactionalProducer.CommitKafkaTransaction(reversedWords);
                lastCommitTime = DateTime.UtcNow;
                reversedWords.Clear();
            }
        }

        public static async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            var config = configuration.GetSection("WordReverser").Get<WordReverserConfig>();
            config.Consumer.ThrowIfContainsNonUserConfigurable();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;

            await Utils.CreateTopicMaybe(adminClient, config.InputTopic.Name, config.InputTopic.NumPartitions);
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            consumer = new ConsumerBuilder<Null, string>(config.Consumer)
                .SetPartitionsLostHandler((c, partitions) => {
                    // When partitions are lost, don't act.
                    logger.LogWarning("Partitions lost.");
                })
                .SetPartitionsRevokedHandler((c, partitions) => {
                    // When partitions are being revoked, commit transaction's remaining partitions
                    // before revoke is completed.
                    logger.LogInformation("Partitions revoked.");
                    if (!cts.IsCancellationRequested)
                    {
                        CommitMaybe();
                    }
                })
                .Build();
    
            transactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = config.OutputTopic.Name,
                RetryInterval = config.RetryInterval,
                LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                ProducerConfig = config.Producer,
                Consumer = consumer,
                CancellationTokenSource = cts
            };

            try
            {
                consumer.Subscribe(config.InputTopic.Name);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<Null, string> consumeResult = consumer.Consume(100);

                        if (consumeResult != null && consumeResult.Message != null)
                        {
                            reversedWords.Add(new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) });
                        }

                        if (reversedWords.Count >= config.CommitMaxMessages ||
                            DateTime.UtcNow > lastCommitTime + config.CommitPeriod)
                        {
                            CommitMaybe();
                        }
                    }
                    catch (KafkaException e)
                    {
                        var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                        logger.LogError($"Consumer KafkaException exception: {e.Message}, TxnRequiresAbort: {txnRequiresAbort}");
                        if (txnRequiresAbort)
                        {
                            AbortTransaction(config.CommitTimeout, config.RetryInterval, cts);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError($"Consumer Exception type: {e.GetType()}");
                if (e is OperationCanceledException)
                {
                    cts.Cancel();
                }
                else
                {
                    logger.LogError($"Caught inner exception: {e.Message}");
                }
            }

            transactionalProducer.Dispose();
            consumer.Close();
            logger.LogInformation($"Process finished");
        }
    }

    public class Utils
    {
        private static ILogger logger = Utils.LoggerFactory.CreateLogger("Utils");

        public static ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
                options.UseUtcTimestamp = true;
            }));

        public static async Task CreateTopicMaybe(IAdminClient adminClient, string name, int partitions)
        {
            try
            {
                await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new TopicSpecification
                    {
                        Name = name,
                        NumPartitions = partitions
                    }
                });
                logger.LogInformation($"Created topic {name}");
            }
            catch (CreateTopicsException e)
            {
                if (e.Error.Code != ErrorCode.Local_Partial || e.Results.Any(r => r.Error.Code != ErrorCode.TopicAlreadyExists))
                {
                    throw e;
                }
                logger.LogInformation($"Topic '{name}' already exists.");
            }
            catch (Exception e)
            {
                logger.LogError($"Error occured creating topic '{name}': {e.Message}.");
                throw;
            }
        }
    }
}
