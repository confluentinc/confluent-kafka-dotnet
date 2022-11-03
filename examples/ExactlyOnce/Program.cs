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
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger<Program>();

        public static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            var completedCancellationTokenSource = new CancellationTokenSource();
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
                    await new WordCreator().Execute(configuration, transactionalId, adminClient, cts);
                    break;
                case "reverse_words":
                    await new WordReverser().Execute(configuration, transactionalId, adminClient, cts);
                    break;
                default:
                    logger.LogError("Unknown command");
                    break;
            }

            completedCancellationTokenSource.Cancel();
        }
    }

    public interface ITransactionalProcess<KC, VC, KP, VP>
    {
        /// <summary>
        ///     Reads messages from the consumer,
        ///     or from something else if no consumer was configured,
        ///     applies required computation on that data and returns the list
        ///     of messages to be sent to the output topic.
        ///     
        ///     Any exception thrown will cause a rewind of the consumer
        ///     and an abort of current transaction.
        /// </summary>
        public List<Message<KP, VP>> ProcessMessages(IConsumer<KC, VC> consumer);

        /// <summary>
        ///     Given current batch of messages to be sent to 
        ///     the output topic, returns true if it's the moment to commit the
        ///     transaction.
        /// </summary>
        public bool IsCommitNeeded(List<Message<KP, VP>> batch);

        /// <summary>
        ///     Called at the end of transaction with the Kafka batch 
        ///     and the <paramref name="committed" /> param that says if
        ///     the transaction completed successfully. Here the application
        ///     must commit or abort any other transactions it has
        ///     ongoing.
        /// </summary>
        public void EndTransaction(List<Message<KP, VP>> batch, bool committed);
    }

    /// <summary>
    ///     This transactional processor allows to use a producer an consumer for transactions,
    ///     by recreating the producer when a fatal error happens, sending offsets to transaction
    ///     and handling aborts and retries correctly.
    /// </summary>
    public sealed class TransactionalProcessor<KC, VC, KP, VP> : IDisposable
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger<TransactionalProcessor<KC, VC, KP, VP>>();

        public IConsumer<KC, VC> Consumer { get; private set; }

        private IProducer<KP, VP> Producer { get; set; }

        private List<Message<KP, VP>> batch = new();

        private bool RecreateProducer = true;

        public string OutputTopic { get; set; }

        public ITransactionalProcess<KC, VC, KP, VP> TransactionalProcess { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig ProducerConfig { get; set; }

        private ConsumerConfig consumerConfig { get; set; }

        public ConsumerConfig ConsumerConfig
        {
            get
            {
                return consumerConfig;
            }
            set
            {
                if (value != null && Consumer == null)
                {
                    consumerConfig = value;
                    Consumer = new ConsumerBuilder<KC, VC>(value)
                        .SetPartitionsAssignedHandler((c, partitions) =>
                        {
                            logger.LogDebug("Assigned {Partitions}", partitions);
                        })
                        .SetPartitionsLostHandler((c, partitions) =>
                        {
                            // When partitions are lost, don't act.
                            logger.LogWarning("Partitions lost.");
                        })
                        .SetPartitionsRevokedHandler((c, partitions) =>
                        {
                            // When partitions are being revoked, commit transaction's remaining partitions
                            // before revoke is completed.
                            logger.LogInformation("Partitions revoked.");
                            if (!CancellationTokenSource.IsCancellationRequested)
                            {
                                CommitMaybe();
                            }
                        })
                        .Build();
                }
            }
        }

        public CancellationTokenSource CancellationTokenSource { get; set; }

        public delegate bool RetryCondition(bool isFatal, bool txnRequiresAbort, bool isRetriable);

        public TransactionalProcessor()
        {
            if (ConsumerConfig != null)
            {
                Consumer = new ConsumerBuilder<KC, VC>(ConsumerConfig)
                    .SetPartitionsLostHandler((c, partitions) =>
                    {
                        // When partitions are lost, don't act.
                        logger.LogWarning("Partitions lost.");
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        // When partitions are being revoked, commit transaction's remaining partitions
                        // before revoke is completed.
                        logger.LogInformation("Partitions revoked.");
                        if (!CancellationTokenSource.IsCancellationRequested)
                        {
                            CommitMaybe();
                        }
                    })
                    .Build();
            }
        }

        public static void ThrowIfNotRetriable(string operation, KafkaException e, TimeSpan retryInterval)
        {
            var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var isRetriable = e is KafkaRetriableException;
            var isFatal = e.Error.IsFatal;
            logger.LogError("{Operation} Kafka Exception caught: '{Message}', IsFatal: {isFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}",
                            operation, e.Message, isFatal, txnRequiresAbort, isRetriable);
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
                    onKafkaException?.Invoke(operation, e, RetryInterval);
                }
            }
        }

        /// <summary>
        ///     Commits transaction's produced and consumed messages.
        ///     Aborts transaction if an abortable exception is thrown,
        ///     recreates the producer if a fatal exception is thrown or
        ///     retries each operation that is throwing a retriable exception.
        /// </summary>
        public void CommitKafkaTransaction(List<Message<KP, VP>> messages)
        {
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (RecreateProducer)
                    {
                        logger.LogInformation("(Re)creating producer");
                        if (Producer != null) Producer.Dispose();
                        Producer = new ProducerBuilder<KP, VP>(ProducerConfig).Build();
                        Producer.InitTransactions(LocalTransactionOperationTimeout);
                        RecreateProducer = false;
                    }

                    logger.LogInformation("Calling BeginTransaction.");
                    Producer.BeginTransaction();

                    logger.LogInformation("Producing {Count} messages.", messages.Count);
                    foreach (var message in messages)
                    {
                        Retry("Produce", () => Producer.Produce(OutputTopic, message), ThrowIfNotRetriable);
                    }

                    logger.LogInformation("Producing message completed");

                    if (Consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            logger.LogInformation("Calling SendOffsetsToTransaction");
                            Producer.SendOffsetsToTransaction(
                                Consumer.Assignment.Select(a => new TopicPartitionOffset(a, Consumer.Position(a))),
                                Consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, ThrowIfNotRetriable);
                        logger.LogInformation("SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        logger.LogInformation("calling CommitTransaction");
                        Producer.CommitTransaction();
                    }, ThrowIfNotRetriable);

                    logger.LogInformation("CommitTransaction completed");
                    break;
                }
                catch (KafkaException e)
                {
                    logger.LogError("Kafka Exception caught, aborting transaction, trying again in {TotalSeconds} seconds: '{Message}'",
                                    RetryInterval.TotalSeconds,
                                    e.Message);
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        RecreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            logger.LogInformation("calling AbortTransaction");
                            Producer.AbortTransaction(LocalTransactionOperationTimeout);
                        }, (operation, eInner, retryInterval) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            logger.LogError("AbortTransaction Kafka Exception caught, trying again in {TotalSeconds} seconds: '{Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbortError}, IsRetriable: {IsRetriable}",
                                            RetryInterval.TotalSeconds, eInner.Message,
                                            IsFatalInner, TxnRequiresAbortErrorInner,
                                            IsRetriableInner);
                            Thread.Sleep(RetryInterval);
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    RecreateProducer = true;
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
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private void RewindConsumer(TimeSpan timeout)
        {
            foreach (var committedOffset in Consumer.Committed(Consumer.Assignment, timeout))
            {
                var position = committedOffset.Offset < 0
                               ? Offset.Beginning : committedOffset.Offset;
                Consumer.Seek(new TopicPartitionOffset(committedOffset.TopicPartition, position));
            }
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This method should commit any application transaction too.
        /// </summary>
        private void CommitMaybe()
        {
            if (batch.Any())
            {
                this.CommitKafkaTransaction(batch);
                TransactionalProcess.EndTransaction(batch, true);
                batch.Clear();
            }
        }

        /// <summary>
        ///     Runs the processor loop, calling process methods.
        /// </summary>
        public void RunLoop()
        {
            batch = new List<Message<KP, VP>>();
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    var messages = TransactionalProcess.ProcessMessages(Consumer);
                    batch.AddRange(messages);
                    var commitNeeded = TransactionalProcess.IsCommitNeeded(batch);
                    if (commitNeeded)
                    {
                        CommitMaybe();
                    }
                }
                catch (KafkaException e)
                {
                    var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    logger.LogError("Consumer KafkaException exception: {Message}, TxnRequiresAbort: {TxnRequiresAbort}",
                                    e.Message, txnRequiresAbort);
                    if (txnRequiresAbort)
                    {
                        if (Consumer != null)
                        {
                            Retry("Rewind",
                                () => RewindConsumer(LocalTransactionOperationTimeout),
                                (operation, e, retryInterval) =>
                                {
                                    Thread.Sleep(RetryInterval);
                                });
                        }
                        TransactionalProcess.EndTransaction(batch, false);
                        batch.Clear();
                        Thread.Sleep(RetryInterval);
                    }
                    else
                    {
                        // This shouldn't happen
                        throw;
                    }
                }
                catch (Exception e)
                {
                    logger.LogError("Process exception: {Message}",
                                    e.Message);
                    // Customer exception requires rewind and abort.
                    if (Consumer != null)
                    {
                        Retry("Rewind",
                            () => RewindConsumer(LocalTransactionOperationTimeout),
                            (operation, e, retryInterval) =>
                            {
                                Thread.Sleep(RetryInterval);
                            });
                    }
                    TransactionalProcess.EndTransaction(batch, false);
                    batch.Clear();
                    Thread.Sleep(RetryInterval);
                }
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            if (Consumer != null) Consumer.Close();
            if (Producer != null) Producer.Dispose();
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

    public class WordCreator : ITransactionalProcess<Null, string, Null, string>
    {
        private readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordCreator));

        private readonly Random random = new();

        private readonly List<string> words = new();

        private WordCreatorConfig config;

        private string CreateWord()
        {
            int start = (int)'a';
            int end = (int)'z';
            int length = random.Next(5, 10);
            var sb = new StringBuilder(10);
            char nextChar = (char)random.Next(start, end + 1);
            for (int i = 0; i < length; i++)
            {
                sb.Append(nextChar);
                nextChar = (char)random.Next(start, end + 1);
            }
            Thread.Sleep(config.ProduceRate);
            return sb.ToString();
        }

        public async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            config = configuration.GetSection("WordCreator").Get<WordCreatorConfig>();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            using var transactionalProcessor =
                    new TransactionalProcessor<Null, string,
                                               Null, string>
                    {
                        OutputTopic = config.OutputTopic.Name,
                        RetryInterval = config.RetryInterval,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        TransactionalProcess = this,
                        ProducerConfig = config.Producer,
                        CancellationTokenSource = cts
                    };
            await Task.Run(() => { transactionalProcessor.RunLoop(); });
            logger.LogInformation("Process finished");
        }

        public List<Message<Null, string>> ProcessMessages(IConsumer<Null, string> consumer)
        {
            if (!words.Any())
            {
                words.Add(CreateWord());
            }
            var ret = new List<Message<Null, string>>();
            foreach (var word in words)
            {
                ret.Add(new Message<Null, string> { Value = word });
            }
            return ret;
        }

        public bool IsCommitNeeded(List<Message<Null, string>> batch)
        {
            return batch.Any();
        }

        public void EndTransaction(List<Message<Null, string>> batch, bool committed)
        {
            if (committed)
            {
                logger.LogInformation("Produced one word");
                words.Clear();
            }
            else
            {
                // Retry the same word
            }
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
    public class WordReverser : ITransactionalProcess<Null, string, Null, string>
    {
        private readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordReverser));

        private DateTime? lastCommitTime = null;

        private WordReverserConfig config;

        private static string Reverse(string original)
        {
            char[] originalChars = original.ToCharArray();
            Array.Reverse(originalChars);
            return new string(originalChars);
        }

        public async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            config = configuration.GetSection("WordReverser").Get<WordReverserConfig>();
            config.Consumer.ThrowIfContainsNonUserConfigurable();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;

            await Utils.CreateTopicMaybe(adminClient, config.InputTopic.Name, config.InputTopic.NumPartitions);
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            using var transactionalProcessor =
                    new TransactionalProcessor<Null, string,
                                               Null, string>
                    {
                        OutputTopic = config.OutputTopic.Name,
                        RetryInterval = config.RetryInterval,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        ProducerConfig = config.Producer,
                        ConsumerConfig = config.Consumer,
                        TransactionalProcess = this,
                        CancellationTokenSource = cts
                    };
            try
            {
                await Task.Run(() =>
                {
                    transactionalProcessor.Consumer.Subscribe(config.InputTopic.Name);
                    lastCommitTime = DateTime.UtcNow;
                    transactionalProcessor.RunLoop();
                });
            }
            catch (Exception e)
            {
                logger.LogError("Consumer Exception type: {Type}", e.GetType());
                if (e is OperationCanceledException)
                {
                    cts.Cancel();
                }
                else
                {
                    logger.LogError("Caught inner exception: {Message}", e.Message);
                }
            }
            logger.LogInformation("Process finished");
        }

        public List<Message<Null, string>> ProcessMessages(IConsumer<Null, string> consumer)
        {
            ConsumeResult<Null, string> consumeResult = consumer.Consume(100);
            var ret = new List<Message<Null, string>>();
            if (consumeResult != null && consumeResult.Message != null)
            {
                ret.Add(new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) });
            }
            return ret;
        }

        public bool IsCommitNeeded(List<Message<Null, string>> batch)
        {
            return batch.Any() &&
                (batch.Count >= config.CommitMaxMessages ||
                DateTime.UtcNow > lastCommitTime + config.CommitPeriod);
        }

        public void EndTransaction(List<Message<Null, string>> batch, bool committed)
        {
            if (committed)
            {
                lastCommitTime = DateTime.UtcNow;
                //  If there's an application transaction involved too, 
                //  this method should commit that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
            else
            {
                //  Retry the same messages after rewind.
                //  If there's an application transaction involved too, 
                //  this method should abort that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
        }
    }

    public class Utils
    {
        public static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
                options.UseUtcTimestamp = true;
            }));

        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger("Utils");

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
                logger.LogInformation("Created topic {Name}", name);
            }
            catch (CreateTopicsException e)
            {
                if (e.Error.Code != ErrorCode.Local_Partial || e.Results.Any(r => r.Error.Code != ErrorCode.TopicAlreadyExists))
                {
                    throw e;
                }
                logger.LogInformation("Topic '{Name}' already exists.", name);
            }
            catch (Exception e)
            {
                logger.LogError("Error occurred creating topic '{Name}': {Message}.", name, e.Message);
                throw;
            }
        }
    }
}
