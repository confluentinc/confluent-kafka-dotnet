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

    /// <summary>
    ///     Error that is thrown if the transactional producer receives a
    ///     fatal error.
    /// </summary>
    public class TransactionalProcessorFatalError : Exception
    {
    }

    /// <summary>
    ///     Represents a single message to be sent
    ///     to an output topic.
    /// </summary>
    public class ProduceMessage<K, V>
    {
        /// <summary>
        ///     Name of the topic to produce to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     Message to produce.
        /// </summary>
        public Message<K, V> Message { get; set; }

        /// <summary>
        ///     Delivery handler for this message.
        /// </summary>
        public Action<DeliveryReport<K, V>> DeliveryHandler { get; set; } = null;
    }

    /// <summary>
    ///     This transactional processor allows to use a producer an consumer for transactions,
    ///     by recreating the producer when a fatal error happens, sending offsets to transaction
    ///     and handling aborts and retries correctly.
    /// </summary>
    public sealed class TransactionalProcessor<KC, VC, KP, VP> : IDisposable
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger<TransactionalProcessor<KC, VC, KP, VP>>();

        private IConsumer<KC, VC> consumer { get; set; }

        private IProducer<KP, VP> producer { get; set; }

        private List<ConsumeResult<KC, VC>> inputBatch = new();

        private List<ProduceMessage<KP, VP>> outputBatch = new();

        private bool recreateProducer = true;

        private ConsumerConfig consumerConfig { get; set; }

        private DateTime? lastEndTransactionTime = null;

        private readonly ICollection<ProduceMessage<KP, VP>> EmptyList =
            new List<ProduceMessage<KP, VP>>(0).AsReadOnly();

        /// <summary>
        ///     Collection of input topic.
        /// </summary>
        public ICollection<string> InputTopics { get; set; }

        /// <summary>
        ///     Interval between retries.
        /// </summary>
        public TimeSpan RetryInterval { get; set; }

        /// <summary>
        ///     Timeout used for transactional operations.
        /// </summary>
        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        /// <summary>
        ///     Producer config.
        /// </summary>
        public ProducerConfig ProducerConfig { get; set; }

        /// <summary>
        ///     Timeout used for consume calls.
        /// </summary>
        public int ConsumeTimeout { get; set; } = 100;

        /// <summary>
        ///     Max messages to be committed.
        /// </summary>
        public int CommitMaxMessages { get; set; } = 1;

        /// <summary>
        ///     Max period between commits.
        /// </summary>
        public TimeSpan CommitPeriod { get; set; } = TimeSpan.MaxValue;

        /// <summary>
        ///     Recreate the producer on fatal errors like 
        ///     producer fenced and continue processing.
        /// </summary>
        public bool RecreateProducerOnFatalErrors { get; set; } = true;

        /// <summary>
        ///     Consumer configuration, don't set for a producer only
        ///     processor.
        /// </summary>
        public ConsumerConfig ConsumerConfig
        {
            get
            {
                return consumerConfig;
            }
            set
            {
                if (value != null && consumer == null)
                {
                    consumerConfig = value;
                    consumer = new ConsumerBuilder<KC, VC>(value)
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

        /// <summary>
        ///     Cancellation token source to stop the processor.
        /// </summary>
        public CancellationTokenSource CancellationTokenSource { get; set; }

        /// <summary>
        ///     Delegate for Subscribe.
        /// </summary>
        public delegate void SubscribeDelegate();

        /// <summary>
        ///     Delegate for Process.
        /// </summary>
        public delegate ICollection<ProduceMessage<KP, VP>> ProcessDelegate(ConsumeResult<KC, VC> consumeResult);

        /// <summary>
        ///     Delegate for IsCommitNeeded.
        /// </summary>
        public delegate bool IsCommitNeededDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                                    ICollection<ProduceMessage<KP, VP>> outputBatch);

        /// <summary>
        ///     Delegate for EndTransaction.
        /// </summary>
        public delegate void EndTransactionDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                                    ICollection<ProduceMessage<KP, VP>> outputBatch,
                                                    bool committed);

        /// <summary>
        ///     Subscribes the consumer to topics.
        /// </summary>
        public SubscribeDelegate Subscribe { get; set; }

        /// <summary>
        ///     Process a new message, or null if no consumer was configured,
        ///     applies required computation on that data and returns the list
        ///     of messages to be sent to the output topic.
        ///     
        ///     Any exception thrown will cause a rewind of the consumer
        ///     and an abort of current transaction.
        /// </summary>
        public ProcessDelegate Process { get; set; }

        /// <summary>
        ///     Given current batch of messages received and to be sent to 
        ///     the output topic, returns true if it's the moment to commit the
        ///     transaction.
        /// </summary>
        public IsCommitNeededDelegate IsCommitNeeded { get; set; }

        /// <summary>
        ///     Called at the end of transaction with the Kafka batch 
        ///     and the <paramref name="committed" /> param that says if
        ///     the transaction completed successfully. Here the application
        ///     must commit or abort any other transactions it has
        ///     ongoing.
        /// </summary>
        public EndTransactionDelegate EndTransaction { get; set; }


        public TransactionalProcessor()
        {
            Subscribe = new SubscribeDelegate(DefaultSubscribe);
            Process = new ProcessDelegate(DefaultProcess);
            IsCommitNeeded = new IsCommitNeededDelegate(DefaultIsCommitNeeded);
            EndTransaction = new EndTransactionDelegate(DefaultEndTransaction);
        }

        private void DefaultSubscribe()
        {
            if (consumer != null)
            {
                consumer.Subscribe(InputTopics);
            }
        }

        private ICollection<ProduceMessage<KP, VP>> DefaultProcess(ConsumeResult<KC, VC> consumeResult)
        {
            return EmptyList;
        }

        private bool DefaultIsCommitNeeded(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch)
        {
            return (inputBatch.Any() || outputBatch.Any()) &&
                (
                    inputBatch.Count >= CommitMaxMessages ||
                    outputBatch.Count >= CommitMaxMessages ||
                    DateTime.UtcNow > lastEndTransactionTime + CommitPeriod
                );
        }

        private void DefaultEndTransaction(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch,
                                           bool committed)
        {
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
        public void CommitKafkaTransaction(ICollection<ProduceMessage<KP, VP>> messages)
        {
            if (ProducerConfig == null) return;

            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (recreateProducer)
                    {
                        logger.LogInformation("(Re)creating producer");
                        if (producer != null) producer.Dispose();
                        producer = new ProducerBuilder<KP, VP>(ProducerConfig).Build();
                        producer.InitTransactions(LocalTransactionOperationTimeout);
                        recreateProducer = false;
                    }

                    logger.LogInformation("Calling BeginTransaction.");
                    producer.BeginTransaction();

                    logger.LogInformation("Producing {Count} messages.", messages.Count);
                    foreach (var message in messages)
                    {
                        Retry("Produce", () => producer.Produce(message.Topic,
                                                                message.Message,
                                                                message.DeliveryHandler), ThrowIfNotRetriable);
                    }

                    logger.LogInformation("Producing message completed");

                    if (consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            logger.LogInformation("Calling SendOffsetsToTransaction");
                            producer.SendOffsetsToTransaction(
                                consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, ThrowIfNotRetriable);
                        logger.LogInformation("SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        logger.LogInformation("calling CommitTransaction");
                        producer.CommitTransaction();
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
                        if (!RecreateProducerOnFatalErrors)
                            throw new TransactionalProcessorFatalError();
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
                            logger.LogError("AbortTransaction Kafka Exception caught, trying again in {TotalSeconds} seconds: '{Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbortError}, IsRetriable: {IsRetriable}",
                                            RetryInterval.TotalSeconds, eInner.Message,
                                            IsFatalInner, TxnRequiresAbortErrorInner,
                                            IsRetriableInner);
                            Thread.Sleep(RetryInterval);
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    if (!RecreateProducerOnFatalErrors)
                                        throw new TransactionalProcessorFatalError();
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
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private void RewindConsumer(TimeSpan timeout)
        {
            foreach (var committedOffset in consumer.Committed(consumer.Assignment, timeout))
            {
                var position = committedOffset.Offset < 0
                               ? Offset.Beginning : committedOffset.Offset;
                consumer.Seek(new TopicPartitionOffset(committedOffset.TopicPartition, position));
            }
        }

        private void ResetTransaction()
        {
            lastEndTransactionTime = DateTime.UtcNow;
            inputBatch.Clear();
            outputBatch.Clear();
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This method should commit any application transaction too.
        /// </summary>
        private void CommitMaybe()
        {
            if (outputBatch.Any())
            {
                CommitKafkaTransaction(outputBatch);
                EndTransaction(inputBatch, outputBatch, true);
                ResetTransaction();
            }
        }

        /// <summary>
        ///     Runs the processor loop, calling process methods.
        /// </summary>
        public void RunLoop()
        {
            ResetTransaction();
            Subscribe();
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<KC, VC> consumeResult = null;
                    if (consumer != null)
                    {
                        consumeResult = consumer.Consume(ConsumeTimeout);
                        inputBatch.Add(consumeResult);
                    }
                    var messages = Process(consumeResult);
                    outputBatch.AddRange(messages);
                    var commitNeeded = IsCommitNeeded(inputBatch, outputBatch);
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
                        if (consumer != null)
                        {
                            Retry("Rewind",
                                () => RewindConsumer(LocalTransactionOperationTimeout),
                                (operation, e, retryInterval) =>
                                {
                                    Thread.Sleep(RetryInterval);
                                });
                        }
                        EndTransaction(inputBatch, outputBatch, false);
                        ResetTransaction();
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
                    if (e is TransactionalProcessorFatalError) throw;

                    logger.LogError("Process exception: {Message}",
                                    e.Message);
                    // Customer exception requires rewind and abort.
                    if (consumer != null)
                    {
                        Retry("Rewind",
                            () => RewindConsumer(LocalTransactionOperationTimeout),
                            (operation, e, retryInterval) =>
                            {
                                Thread.Sleep(RetryInterval);
                            });
                    }
                    EndTransaction(inputBatch, outputBatch, false);
                    ResetTransaction();
                    Thread.Sleep(RetryInterval);
                }
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            if (consumer != null) consumer.Close();
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

    public class WordCreator
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
                        RetryInterval = config.RetryInterval,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        Process = Process,
                        EndTransaction = EndTransaction,
                        ProducerConfig = config.Producer,
                        CancellationTokenSource = cts
                    };
            await Task.Run(() => { transactionalProcessor.RunLoop(); });
            logger.LogInformation("Process finished");
        }

        public List<ProduceMessage<Null, string>> Process(ConsumeResult<Null, string> _)
        {
            if (!words.Any())
            {
                words.Add(CreateWord());
            }
            var ret = new List<ProduceMessage<Null, string>>();
            foreach (var word in words)
            {
                ret.Add(new ProduceMessage<Null, string>()
                {
                    Topic = config.OutputTopic.Name,
                    Message = new Message<Null, string> { Value = word }
                });
            }
            return ret;
        }

        public void EndTransaction(ICollection<ConsumeResult<Null, string>> _,
                                   ICollection<ProduceMessage<Null, string>> _1,
                                   bool committed)
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
    public class WordReverser
    {
        private readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordReverser));

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
                        RetryInterval = config.RetryInterval,
                        ProducerConfig = config.Producer,
                        ConsumerConfig = config.Consumer,
                        CommitMaxMessages = config.CommitMaxMessages,
                        CommitPeriod = config.CommitPeriod,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        InputTopics = new List<string>()
                        {
                            config.InputTopic.Name
                        },
                        Process = Process,
                        EndTransaction = EndTransaction,
                        CancellationTokenSource = cts
                    };
            try
            {
                await Task.Run(() =>
                {
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

        public List<ProduceMessage<Null, string>> Process(ConsumeResult<Null, string> consumeResult)
        {
            var ret = new List<ProduceMessage<Null, string>>();
            if (consumeResult != null && consumeResult.Message != null)
            {
                ret.Add(new ProduceMessage<Null, string>()
                {
                    Topic = config.OutputTopic.Name,
                    Message = new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) }
                });
            }
            return ret;
        }

        public static void EndTransaction(ICollection<ConsumeResult<Null, string>> _,
                                          ICollection<ProduceMessage<Null, string>> _1,
                                          bool committed)
        {
            if (committed)
            {
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
