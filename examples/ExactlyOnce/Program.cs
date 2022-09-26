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

        /// <summary>
        ///     Logs provided <paramref name="message"/>.
        /// </summary>
        public static void Log(string message)
        {
            Console.Error.WriteLine($"{DateTimeOffset.Now.ToUnixTimeSeconds()}|EXAMPLE|{message}");
        }

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
                Log($"Created topic {name}");
            }
            catch (CreateTopicsException)
            {
                // Continue if topic already exists
                Log($"Topic {name} already exists");
            }
            catch (Exception e)
            {
                Log($"CreateTopic caught a different type of exception: \"{e.Message}\", this shouldn't happen'");
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
                        Log = Program.Log,
                        CancellationTokenSource = cts
                    }
                    .Run();
                    break;
                case "reverse_words":
                    wordReverserConfig.Producer.TransactionalId = transactionalId;
                    new WordReverser
                    {
                        Config = wordReverserConfig,
                        Log = Program.Log,
                        CancellationTokenSource = cts
                    }
                    .Run();
                    break;
                default:
                    Log("Unknown command");
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

        public Action<string> Log { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig ProducerConfig { get; set; }

        public IConsumer<K, V> Consumer { get; set; }

        private bool ToRecreateProducer = true;

        private IProducer<K, V> Producer { get; set; }

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
            Log($"{operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
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
            Log($"{operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
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
            Log("(Re)Creating producer");
            if (Producer != null) Producer.Dispose();
            Producer = new ProducerBuilder<K, V>(ProducerConfig).Build();
            Producer.InitTransactions(LocalTransactionOperationTimeout);
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
                    Log($"{operation} caught a different type of exception, this shouldn't happen'");
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
                    if (ToRecreateProducer)
                    {
                        RecreateProducer();
                        ToRecreateProducer = false;
                    }
                    try
                    {
                        Log("Calling BeginTransaction");
                        Producer.BeginTransaction();
                    }
                    catch (Exception e)
                    {
                        Log($"BeginTransaction threw: {e.Message}");
                        throw;
                    }

                    Log($"Transaction contains {messages.Count} messages");


                    Log("Calling Produce many times");
                    foreach (var message in messages)
                    {
                        Retry("Produce", () =>
                        {
                            Producer.Produce(OutputTopic, message);
                        }, RetryUnlessFatalOrAbortable);
                    }

                    Log($"Produce completed");

                    if (Consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            Log("Calling SendOffsetsToTransaction");
                            Producer.SendOffsetsToTransaction(
                                Consumer.Assignment.Select(a => new TopicPartitionOffset(a, Consumer.Position(a))),
                                Consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, RetryIfNeeded);
                        Log($"SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        Log("calling CommitTransaction");
                        Producer.CommitTransaction();
                    }, RetryIfNeeded);

                    Log($"CommitTransaction completed");
                    break;
                }
                catch (KafkaException e)
                {
                    Log($"Kafka Exception caught, aborting transaction, trying again in {RetryInterval.TotalSeconds} seconds: '{e.Message}'");
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        ToRecreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            Log("calling AbortTransaction");
                            Producer.AbortTransaction(LocalTransactionOperationTimeout);
                        }, (string operation, KafkaException eInner) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            Log($"AbortTransaction Kafka Exception caught, trying again in {RetryInterval.TotalSeconds} seconds: '{eInner.Message}', IsFatal: {IsFatalInner}, TxnRequiresAbort: {TxnRequiresAbortErrorInner}, IsRetriable: {IsRetriableInner}");
                            RetryAfterSleeping();
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    ToRecreateProducer = true;
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
                    Log("Caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            if (Producer != null) Producer.Dispose();
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

        public Action<string> Log { get; set; }

        public CancellationTokenSource CancellationTokenSource { get; set; }

        private static readonly Random random = new Random();

        private TransactionalProducer<Null, string> TransactionalProducer { get; set; }

        private List<Message<Null, string>> CreatedWords = new List<Message<Null, string>>();

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
            TransactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = Config.OutputTopic,
                RetryInterval = Config.RetryInterval,
                LocalTransactionOperationTimeout = Config.LocalTransactionOperationTimeout,
                ProducerConfig = Config.Producer,
                CancellationTokenSource = CancellationTokenSource,
                Log = Log
            };
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (!CreatedWords.Any())
                    {
                        CreatedWords.Add(new Message<Null, string> { Value = CreateWord() });
                    }
                    TransactionalProducer.CommitKafkaTransaction(CreatedWords);
                    CreatedWords.Clear();
                    Log($"Produced one word");
                }
                catch (KafkaException)
                {
                    // Retry the same word
                }
                Thread.Sleep(Config.ProduceRate);
            }

            TransactionalProducer.Dispose();
            Log($"Process finished");
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

        private TransactionalProducer<Null, string> TransactionalProducer { get; set; }

        private IConsumer<Null, string> Consumer { get; set; }

        private List<Message<Null, string>> ReversedWords = new List<Message<Null, string>>();

        private DateTime? LastCommit = null;

        public CancellationTokenSource CancellationTokenSource { get; set; }

        public Action<string> Log { get; set; }

        /// <summary>
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private void RewindConsumer(TimeSpan timeout)
        {
            var committedOffsets = Consumer.Committed(Consumer.Assignment, timeout);
            committedOffsets = committedOffsets.Select(committed =>
            {
                var position = Consumer.Position(committed.TopicPartition);
                position = position < 0 ? -2 : position;
                return new TopicPartitionOffset(committed.TopicPartition, position);
            }).ToList();
            foreach (var committedOffset in committedOffsets)
            {
                Consumer.Seek(committedOffset);
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
                    Log($"{operation} caught a different type of exception, this shouldn't happen'");
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
            Log("Partitions revoked.");
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
            Log("Partitions lost.");
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
            ReversedWords.Clear();
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
            if (LastCommit != null) return;
            LastCommit = DateTime.UtcNow;

            Consumer = new ConsumerBuilder<Null, string>(Config.Consumer)
                .SetPartitionsLostHandler(PartitionsLostHandler)
                .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                .Build();
            TransactionalProducer = new TransactionalProducer<Null, string>
            {
                OutputTopic = Config.OutputTopic,
                RetryInterval = Config.RetryInterval,
                LocalTransactionOperationTimeout = Config.LocalTransactionOperationTimeout,
                ProducerConfig = Config.Producer,
                Consumer = Consumer,
                CancellationTokenSource = CancellationTokenSource,
                Log = Log
            };

            try
            {
                Consumer.Subscribe(Config.InputTopic);
                while (!CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<Null, string> consumeResult = Consumer.Consume(100);

                        if (consumeResult != null && consumeResult.Message != null)
                        {
                            ReversedWords.Add(new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) });
                        }

                        if (ReversedWords.Count >= Config.CommitMaxMessages ||
                            DateTime.UtcNow > LastCommit + Config.CommitPeriod)
                        {
                            CheckCommit();
                        }
                    }
                    catch (KafkaException e)
                    {
                        var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                        Log($"Consumer KafkaException exception: {e.Message}, TxnRequiresAbort: {txnRequiresAbort}");
                        if (txnRequiresAbort)
                        {
                            AbortTransaction();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log($"Consumer Exception type: {e.GetType()}");
                if (e is OperationCanceledException)
                {
                    CancellationTokenSource.Cancel();
                }
                else
                {
                    Log($"Caught inner exception: {e.Message}");
                }
            }

            TransactionalProducer.Dispose();
            Consumer.Close();
            Log($"Process finished");
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This should commit any application transaction too.
        /// </summary>
        private void CheckCommit()
        {
            if (ReversedWords.Any())
            {
                TransactionalProducer.CommitKafkaTransaction(ReversedWords);
                LastCommit = DateTime.UtcNow;
                ReversedWords.Clear();
            }
        }
    }
}
