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
    /// <summary>
    ///     Main class of this EOS producer example.
    /// </summary>
    public class Program
    {

        public static ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
                options.UseUtcTimestamp = true;
            }));

        private static ILogger logger = LoggerFactory.CreateLogger<Program>();

        private static async Task CreateTopic(IAdminClient adminClient, string name, int partitions)
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
            catch (CreateTopicsException)
            {
                // Continue if topic already exists
                logger.LogInformation($"Topic {name} already exists");
            }
            catch (Exception e)
            {
                logger.LogError($"CreateTopic caught a different type of exception: \"{e.Message}\", this shouldn't happen'");
                throw;
            }
        }

        /// <summary>
        ///     Main method.
        /// </summary>
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
                Console.WriteLine("create_words <transactional-id>");
                Console.WriteLine("reverse_words <transactional-id>");
                completedCancellationTokenSource.Cancel();
                return;
            }

            var command = args[0];
            var transactionalId = args[1];

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("./appsettings.json")
                .Build();

            var wordCreatorConfig = configuration.GetSection("WordCreator").Get<WordCreatorConfig>();
            var wordReverserConfig = configuration.GetSection("WordReverser").Get<WordReverserConfig>();
            var adminClientConfig = configuration.GetSection("AdminClient").Get<AdminClientConfig>();

            wordCreatorConfig.Producer.ThrowIfContainsNonUserConfigurable();
            wordReverserConfig.Producer.ThrowIfContainsNonUserConfigurable();
            wordReverserConfig.Consumer.ThrowIfContainsNonUserConfigurable();

            var adminClient = new AdminClientBuilder(adminClientConfig).Build();
            await CreateTopic(adminClient, wordCreatorConfig.OutputTopic, wordCreatorConfig.TopicPartitions);
            await CreateTopic(adminClient, wordReverserConfig.OutputTopic, wordReverserConfig.TopicPartitions);

            switch (command)
            {
                case "create_words":
                    wordCreatorConfig.Producer.TransactionalId = transactionalId;
                    new WordCreator
                    {
                        Config = wordCreatorConfig,
                        CancellationTokenSource = cts
                    }
                    .Run();
                    break;
                case "reverse_words":
                    wordReverserConfig.Producer.TransactionalId = transactionalId;
                    new WordReverser
                    {
                        Config = wordReverserConfig,
                        CancellationTokenSource = cts
                    }
                    .Run();
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
        public string OutputTopic { get; set; }

        private ILogger logger = Program.LoggerFactory.CreateLogger<TransactionalProducer<K, V>>();

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig ProducerConfig { get; set; }

        public IConsumer<K, V> Consumer { get; set; }

        private bool toRecreateProducer = true;

        private IProducer<K, V> producer { get; set; }

        public CancellationTokenSource CancellationTokenSource { get; set; }

        /// <summary>
        ///     Retry callback that sleeps for a configured period.
        /// </summary>
        private void RetryAfterSleeping(string operation = null, KafkaException e = null)
        {
            Thread.Sleep(RetryInterval);
        }

        /// <summary>
        ///     Retry callback that stops retries if
        ///     the error is fatal or abortable or not retriable.
        /// </summary>
        private void RetryIfNeeded(string operation, KafkaException e)
        {
            var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var IsRetriable = e is KafkaRetriableException;
            var IsFatal = e.Error.IsFatal;
            logger.LogError($"{operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
            if (IsFatal || TxnRequiresAbort || !IsRetriable)
            {
                throw e;
            }
            RetryAfterSleeping();
        }

        /// <summary>
        ///     Retry callback that stops retries only if
        ///     the error is fatal or abortable.
        /// </summary>
        private void RetryUnlessFatalOrAbortable(string operation, KafkaException e)
        {
            var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var IsRetriable = e is KafkaRetriableException;
            var IsFatal = e.Error.IsFatal;
            logger.LogError($"{operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
            if (IsFatal || TxnRequiresAbort)
            {
                throw e;
            }
            RetryAfterSleeping();
        }

        /// <summary>
        ///     Recreates the producer after disposing the old one
        ///     and initializes transactions.
        /// </summary>
        private void RecreateProducer()
        {
            logger.LogInformation("(Re)Creating producer");
            if (producer != null) producer.Dispose();
            producer = new ProducerBuilder<K, V>(ProducerConfig).Build();
            producer.InitTransactions(LocalTransactionOperationTimeout);
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds,
        ///     call <paramref name="onKafkaException"/> if a <see cref="Confluent.Kafka.KafkaException" /> occurs.
        /// </summary>
        private void Retry(string operation, Action action, Action<string, KafkaException> onKafkaException = null)
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
                        onKafkaException(operation, e);
                    }
                }
                catch
                {
                    logger.LogError($"{operation} caught a different type of exception, this shouldn't happen'");
                    throw;
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
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (toRecreateProducer)
                    {
                        RecreateProducer();
                        toRecreateProducer = false;
                    }
                    try
                    {
                        logger.LogInformation("Calling BeginTransaction");
                        producer.BeginTransaction();
                    }
                    catch (Exception e)
                    {
                        logger.LogError($"BeginTransaction threw: {e.Message}");
                        throw;
                    }

                    logger.LogInformation($"Transaction contains {messages.Count} messages");


                    logger.LogInformation("Calling Produce many times");
                    foreach (var message in messages)
                    {
                        Retry("Produce", () =>
                        {
                            producer.Produce(OutputTopic, message);
                        }, RetryUnlessFatalOrAbortable);
                    }

                    logger.LogInformation($"Produce completed");

                    if (Consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            logger.LogInformation("Calling SendOffsetsToTransaction");
                            producer.SendOffsetsToTransaction(
                                Consumer.Assignment.Select(a => new TopicPartitionOffset(a, Consumer.Position(a))),
                                Consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, RetryIfNeeded);
                        logger.LogInformation($"SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        logger.LogInformation("calling CommitTransaction");
                        producer.CommitTransaction();
                    }, RetryIfNeeded);

                    logger.LogInformation($"CommitTransaction completed");
                    break;
                }
                catch (KafkaException e)
                {
                    logger.LogError($"Kafka Exception caught, aborting transaction, trying again in {RetryInterval.TotalSeconds} seconds: '{e.Message}'");
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        toRecreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            logger.LogInformation("calling AbortTransaction");
                            producer.AbortTransaction(LocalTransactionOperationTimeout);
                        }, (string operation, KafkaException eInner) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            logger.LogError($"AbortTransaction Kafka Exception caught, trying again in {RetryInterval.TotalSeconds} seconds: '{eInner.Message}', IsFatal: {IsFatalInner}, TxnRequiresAbort: {TxnRequiresAbortErrorInner}, IsRetriable: {IsRetriableInner}");
                            RetryAfterSleeping();
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    toRecreateProducer = true;
                                }
                                // Propagate abort to consumer and application
                                throw e;
                            }
                        });
                        // Propagate abort to consumer and application
                        throw;
                    }
                    RetryAfterSleeping();
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

        public string OutputTopic { get; set; }

        public int TopicPartitions { get; set; }

        public TimeSpan ProduceRate { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }
    }

    /// <summary>
    ///     Creates random words and sends them to a topic.
    /// </summary>
    public class WordCreator
    {

        public WordCreatorConfig Config { get; set; }

        private ILogger logger = Program.LoggerFactory.CreateLogger<WordCreator>();

        public CancellationTokenSource CancellationTokenSource { get; set; }

        private static readonly Random random = new Random();

        private TransactionalProducer<Null, string> transactionalProducer { get; set; }

        private List<Message<Null, string>> createdWords = new List<Message<Null, string>>();

        private string CreateWord()
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

        public void Run()
        {
            transactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = Config.OutputTopic,
                RetryInterval = Config.RetryInterval,
                LocalTransactionOperationTimeout = Config.LocalTransactionOperationTimeout,
                ProducerConfig = Config.Producer,
                CancellationTokenSource = CancellationTokenSource
            };
            while (!CancellationTokenSource.IsCancellationRequested)
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
                Thread.Sleep(Config.ProduceRate);
            }

            transactionalProducer.Dispose();
            logger.LogInformation($"Process finished");
        }
    }


    public class WordReverserConfig
    {

        public string InputTopic { get; set; }

        public string OutputTopic { get; set; }

        public int TopicPartitions { get; set; }

        public int CommitMaxMessages { get; set; }

        public TimeSpan CommitPeriod { get; set; }

        public TimeSpan CommitTimeout { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }

        public ConsumerConfig Consumer { get; set; }
    }

    /// <summary>
    ///     Reads words from a topic, reverses and sends them to a different topic with EOS.
    /// </summary>
    public class WordReverser
    {

        public WordReverserConfig Config { get; set; }

        private TransactionalProducer<Null, string> transactionalProducer { get; set; }

        private IConsumer<Null, string> consumer { get; set; }

        private List<Message<Null, string>> reversedWords = new List<Message<Null, string>>();

        private DateTime? lastCommit = null;

        public CancellationTokenSource CancellationTokenSource { get; set; }

        private ILogger logger = Program.LoggerFactory.CreateLogger<WordReverser>();

        /// <summary>
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private void RewindConsumer(TimeSpan timeout)
        {
            var committedOffsets = consumer.Committed(consumer.Assignment, timeout);
            committedOffsets = committedOffsets.Select(committed =>
            {
                var position = consumer.Position(committed.TopicPartition);
                position = position < 0 ? -2 : position;
                return new TopicPartitionOffset(committed.TopicPartition, position);
            }).ToList();
            foreach (var committedOffset in committedOffsets)
            {
                consumer.Seek(committedOffset);
            }
        }

        /// <summary>
        ///     Retry callback that sleeps for a configured period.
        /// </summary>
        private void RetryAfterSleeping(string operation = null, KafkaException e = null)
        {
            Thread.Sleep(Config.RetryInterval);
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds,
        ///     call <paramref name="onKafkaException"/> if a <see cref="Confluent.Kafka.KafkaException" /> occurs.
        /// </summary>
        private void Retry(string operation, Action action, Action<string, KafkaException> onKafkaException = null)
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
                        onKafkaException(operation, e);
                    }
                }
                catch
                {
                    logger.LogError($"{operation} caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }
        }

        /// <summary>
        ///     When partitions are being revoked, commit transaction's remaining partitions
        ///     before revoke is completed.
        /// </summary>
        private void PartitionsRevokedHandler(IConsumer<Null, string> consumer, List<TopicPartitionOffset> partitions)
        {
            logger.LogInformation("Partitions revoked.");
            if (!CancellationTokenSource.IsCancellationRequested)
            {
                CheckCommit();
            }
        }

        /// <summary>
        ///     When partitions are lost, don't act.
        /// </summary>
        private void PartitionsLostHandler(IConsumer<Null, string> consumer, List<TopicPartitionOffset> partitions)
        {
            logger.LogWarning("Partitions lost.");
        }

        private string Reverse(string original)
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
        private void AbortTransaction()
        {
            reversedWords.Clear();
            Retry("Consumer", () =>
            {
                RewindConsumer(Config.CommitTimeout);
            }, RetryAfterSleeping);
        }

        /// <summary>
        ///     Starts EOS consumer and producer main loop.
        /// </summary>
        public void Run()
        {
            if (lastCommit != null) return;
            lastCommit = DateTime.UtcNow;

            consumer = new ConsumerBuilder<Null, string>(Config.Consumer)
                .SetPartitionsLostHandler(PartitionsLostHandler)
                .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                .Build();
            transactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = Config.OutputTopic,
                RetryInterval = Config.RetryInterval,
                LocalTransactionOperationTimeout = Config.LocalTransactionOperationTimeout,
                ProducerConfig = Config.Producer,
                Consumer = consumer,
                CancellationTokenSource = CancellationTokenSource
            };

            try
            {
                consumer.Subscribe(Config.InputTopic);
                while (!CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<Null, string> consumeResult = consumer.Consume(100);

                        if (consumeResult != null && consumeResult.Message != null)
                        {
                            reversedWords.Add(new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) });
                        }

                        if (reversedWords.Count >= Config.CommitMaxMessages ||
                            DateTime.UtcNow > lastCommit + Config.CommitPeriod)
                        {
                            CheckCommit();
                        }
                    }
                    catch (KafkaException e)
                    {
                        var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                        logger.LogError($"Consumer KafkaException exception: {e.Message}, TxnRequiresAbort: {txnRequiresAbort}");
                        if (txnRequiresAbort)
                        {
                            AbortTransaction();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError($"Consumer Exception type: {e.GetType()}");
                if (e is OperationCanceledException)
                {
                    CancellationTokenSource.Cancel();
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

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This should commit any application transaction too.
        /// </summary>
        private void CheckCommit()
        {
            if (reversedWords.Any())
            {
                transactionalProducer.CommitKafkaTransaction(reversedWords);
                lastCommit = DateTime.UtcNow;
                reversedWords.Clear();
            }
        }
    }
}
