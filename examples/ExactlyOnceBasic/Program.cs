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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///     This example demonstrates a basic Exactly-once semantics producer moving messages from
///     an input topic to an output topic. To run the example follow these steps: 
///
///     1. create input topic with: createtopic <input_topic> <num_partitions>
///     2. create output topic with: createtopic <output_topic> <num_partitions>
///     3. generate messages and send them to the input topic: generate <input_topic> <msgs_per_second>
///     4. process messages and copy them to the output topic: process <input_topic> <output_topic> <commit_period_seconds>
/// </summary>
namespace Confluent.Kafka.Examples.ExactlyOnceBasic
{

    public class Program
    {

        public static async Task Main(string[] args)
        {
            var r = new Random();

            if (args.Length == 0)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  .. createtopic <name> <num_partitions>");
                Console.WriteLine("  .. deletetopic <name>");
                Console.WriteLine("  .. process <input_topic> <output_topic> <commit_period_seconds>");
                Console.WriteLine("  .. generate <topic> <msgs_per_second>");
                Console.WriteLine("  required env vars: TE_BOOTSTRAP_SERVERS, TE_SASL_USERNAME, TE_SASL_PASSWORD");
                Console.WriteLine("  optional env vars: TE_ADMINCLIENT_DEBUG, TE_PRODUCER_DEBUG, TE_CONSUMER_DEBUG");
            }

            else if (args[0] == "createtopic")
            {
                var adminConfig = ProcessConfig.AdminConfig;
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                await adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = args[1], NumPartitions = int.Parse(args[2]) } });
            }

            else if (args[0] == "deletetopic")
            {
                var adminConfig = ProcessConfig.AdminConfig;
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                await adminClient.DeleteTopicsAsync(new List<string> { args[1] });
            }

            else if (args[0] == "generate")
            {
                var topic = args[1];
                var msgsPerSecond = int.Parse(args[2]);
                new OrderGenerator
                {
                    Topic = topic,
                    MsgsPerSecond = msgsPerSecond
                }.Generate();
            }

            else if (args[0] == "process")
            {
                ProcessConfig.InputTopic = args[1];
                ProcessConfig.OutputTopic = args[2];
                ProcessConfig.CommitPeriodSeconds = int.Parse(args[3]);

                await new HostBuilder()
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddHostedService<OrderProcessingWorker>();
                    })
                    .RunConsoleAsync();
            }
            else
            {
                Console.WriteLine($"unknown command {args[0]}");
            }
        }
    }


    class ProcessConfig
    {
        public static string InputTopic { get; set; }
        public static string OutputTopic { get; set; }
        public static int CommitPeriodSeconds { get; set; }

        private static readonly string DefaultConsumerGroupId = "trading-engine-group";

        private static void ConfigureSASL(Dictionary<string, string> config, string username, string password)
        {
            if (username != null)
            {
                config.Add("sasl.username", username);
                config.Add("sasl.password", password);
                config.Add("sasl.mechanism", "PLAIN");
                config.Add("security.protocol", "SASL_SSL");
            }
        }

        public static Dictionary<string, string> AdminConfig
        {
            get
            {
                var bootstrapServers = Environment.GetEnvironmentVariable("TE_BOOTSTRAP_SERVERS");
                var testAdminClientDebug = Environment.GetEnvironmentVariable("TE_ADMINCLIENT_DEBUG");
                var testSASLUsername = Environment.GetEnvironmentVariable("TE_SASL_USERNAME");
                var testSASLPassword = Environment.GetEnvironmentVariable("TE_SASL_PASSWORD");

                var adminConfig = new Dictionary<string, string>
                {
                    {"bootstrap.servers", bootstrapServers},
                };
                if (testAdminClientDebug != null)
                {
                    adminConfig.Add("debug", testAdminClientDebug);
                }
                ConfigureSASL(adminConfig, testSASLUsername, testSASLPassword);
                return adminConfig;
            }
        }

        public static Dictionary<string, string> SourceProducerConfig
        {
            get
            {
                var bootstrapServers = Environment.GetEnvironmentVariable("TE_BOOTSTRAP_SERVERS");
                var testTransactionTimeoutMs = Environment.GetEnvironmentVariable("TE_TRANSACTION_TIMEOUT_MS");
                var testProducerDebug = Environment.GetEnvironmentVariable("TE_PRODUCER_DEBUG");
                var testSASLUsername = Environment.GetEnvironmentVariable("TE_SASL_USERNAME");
                var testSASLPassword = Environment.GetEnvironmentVariable("TE_SASL_PASSWORD");

                var producerConfig = new Dictionary<string, string>
                {
                    {"bootstrap.servers", bootstrapServers},
                    {"compression.type", "gzip"},
                    {"acks", "all"}
                };
                if (testProducerDebug != null)
                {
                    producerConfig.Add("debug", testProducerDebug);
                }
                ConfigureSASL(producerConfig, testSASLUsername, testSASLPassword);
                return producerConfig;
            }
        }

        public static Dictionary<string, string> TargetProducerConfig
        {
            get
            {
                var bootstrapServers = Environment.GetEnvironmentVariable("TE_BOOTSTRAP_SERVERS");
                var testTransactionTimeoutMs = Environment.GetEnvironmentVariable("TE_TRANSACTION_TIMEOUT_MS");
                var testProducerDebug = Environment.GetEnvironmentVariable("TE_PRODUCER_DEBUG");
                var testSASLUsername = Environment.GetEnvironmentVariable("TE_SASL_USERNAME");
                var testSASLPassword = Environment.GetEnvironmentVariable("TE_SASL_PASSWORD");
                var producerConfig = new Dictionary<string, string>
                {
                    {"bootstrap.servers", bootstrapServers},
                    {"transactional.id", "trading-engine-tid-" + Guid.NewGuid().ToString()},
                    {"linger.ms", "0"},
                    {"compression.type", "gzip"},
                    {"statistics.interval.ms", "15000"},
                    {"message.timeout.ms", "10000"},
                    {"acks", "all"},
                };
                if (testProducerDebug != null)
                {
                    producerConfig.Add("debug", testProducerDebug);
                }
                if (testTransactionTimeoutMs != null)
                {
                    producerConfig.Add("transaction.timeout.ms", testTransactionTimeoutMs);
                }
                ConfigureSASL(producerConfig, testSASLUsername, testSASLPassword);
                return producerConfig;
            }
        }


        public static Dictionary<string, string> ConsumerConfig
        {
            get
            {
                var bootstrapServers = Environment.GetEnvironmentVariable("TE_BOOTSTRAP_SERVERS");
                var testConsumerDebug = Environment.GetEnvironmentVariable("TE_CONSUMER_DEBUG");
                var testSASLUsername = Environment.GetEnvironmentVariable("TE_SASL_USERNAME");
                var testSASLPassword = Environment.GetEnvironmentVariable("TE_SASL_PASSWORD");
                var testSessionTimeoutMs = Environment.GetEnvironmentVariable("TE_SESSION_TIMEOUT_MS");
                var testGroupId = Environment.GetEnvironmentVariable("TE_GROUP_ID");

                testGroupId = testGroupId ?? DefaultConsumerGroupId;
                var consumerConfig = new Dictionary<string, string>
                {
                    {"bootstrap.servers", bootstrapServers},
                    {"enable.auto.commit", "false"},
                    {"fetch.min.bytes", "1"},
                    {"group.id", testGroupId},
                    {"auto.offset.reset", "earliest"},
                };
                if (testConsumerDebug != null)
                {
                    consumerConfig.Add("debug", testConsumerDebug);
                }
                if (testSessionTimeoutMs != null)
                {
                    consumerConfig.Add("session.timeout.ms", testSessionTimeoutMs);
                    consumerConfig.Add("max.poll.interval.ms", testSessionTimeoutMs);
                }
                ConfigureSASL(consumerConfig, testSASLUsername, testSASLPassword);
                return consumerConfig;
            }
        }
    }

    public class OrderGenerator
    {
        private static readonly Random random = new Random();

        public string Topic { get; set; }

        public int MsgsPerSecond { get; set; }

        string MakeMessage(int length)
        {
            var result = "";
            for (int i = 0; i < length; ++i)
            {
                result += "" + random.Next(9);
            }
            return result;
        }

        public void Generate()
        {
            var producerConfig = ProcessConfig.SourceProducerConfig;
            var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            var startTime = DateTime.Now;
            long producedCount = 0;
            while (true)
            {
                var elapsedSeconds = (DateTime.Now - startTime).TotalSeconds;
                long expectedNumberProduced = (long)(elapsedSeconds * MsgsPerSecond);
                if (expectedNumberProduced <= producedCount)
                {
                    System.Threading.Thread.Sleep(5);
                    continue;
                }
                for (long i = 0; i < (expectedNumberProduced - producedCount); ++i)
                {
                    producer.Produce(Topic, new Message<string, string> { Key = MakeMessage(100), Value = MakeMessage(100) });
                    producedCount += 1;
                    if (producedCount % 1000 == 0)
                    {
                        Console.Error.WriteLine($"TESTAPP Produced {producedCount} messages");
                    }
                }
            }
        }
    }

    public class OrderProcessingWorker : IHostedService
    {
        private readonly CancellationTokenSource _stoppingToken;
        private bool _isProcessFinished;
        private bool _isConsumerClosing;
        private IProducer<string, string> _producer;
        private static readonly TimeSpan TransactionTimeout = TimeSpan.FromSeconds(10);
        private System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();

        private List<KeyValuePair<string, string>> _eventStore = new List<KeyValuePair<string, string>>();
        private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(100);

        private static readonly TimeSpan CommittedTimeout = TimeSpan.FromMilliseconds(5000);

        private static readonly TimeSpan RetryAfterMs = TimeSpan.FromMilliseconds(100);

        private Dictionary<string, string> producerConfig;

        private bool recreateProducer = false;


        public OrderProcessingWorker()
        {
            _stoppingToken = new CancellationTokenSource();
            producerConfig = ProcessConfig.TargetProducerConfig;
            RecreateProducer();
        }

        private void RetryIfNeeded(string operation, KafkaException e)
        {
            var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var IsRetriable = e is KafkaRetriableException;
            var IsFatal = e.Error.IsFatal;
            Log($"TESTAPP {operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
            if (IsFatal || TxnRequiresAbort || !IsRetriable)
            {
                throw e;
            }
            RetryAfterSleeping();
        }

        private void RetryUnlessFatalOrAborted(string operation, KafkaException e)
        {
            var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var IsRetriable = e is KafkaRetriableException;
            var IsFatal = e.Error.IsFatal;
            Log($"TESTAPP {operation} Kafka Exception caught: '{e.Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}");
            if (IsFatal || TxnRequiresAbort)
            {
                throw e;
            }
            RetryAfterSleeping();
        }

        private void RetryAfterSleeping(string operation = null, KafkaException e = null)
        {
            Thread.Sleep(RetryAfterMs);
        }

        private void RecreateProducer()
        {
            Log("TESTAPP (Re)Creating producer");
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
            _producer.InitTransactions(TimeSpan.FromSeconds(30));
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Log("TESTAPP StartAsync");
            await Task.Delay(5, cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
            {
                _ = Task.Run(TradingJob, CancellationToken.None);
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Log("TESTAPP StopAsync");

            _stoppingToken.Cancel();

            while (!_isProcessFinished)
            {
                await Task.Delay(50, CancellationToken.None);
            }
        }

        private void RewindConsumer<K, V>(IConsumer<K, V> consumer, TimeSpan timeout)
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

        private void TradingJob()
        {
            DateTime lastTransactionCommit = DateTime.UtcNow;

            try
            {
                var consumerConfig = ProcessConfig.ConsumerConfig;

                ConsumerBuilder<string, string> consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig)
                    .SetPartitionsLostHandler(PartitionsLostHandler)
                    .SetPartitionsRevokedHandler(PartitionsRevokedHandler);

                using (IConsumer<string, string> consumer = consumerBuilder.Build())
                {
                    _isConsumerClosing = false;

                    consumer.Subscribe(ProcessConfig.InputTopic);

                    while (!_isProcessFinished)
                    {
                        try
                        {
                            _stoppingToken.Token.ThrowIfCancellationRequested();

                            stopwatch.Reset();
                            stopwatch.Start();
                            ConsumeResult<string, string> consumeResult = consumer.Consume(ConsumeTimeout);
                            stopwatch.Stop();

                            if (consumeResult != null && consumeResult.Message != null)
                            {
                                _eventStore.Add(new KeyValuePair<string, string>(consumeResult.Message.Key, consumeResult.Message.Value));
                            }

                            if (DateTime.UtcNow > lastTransactionCommit + TimeSpan.FromSeconds(ProcessConfig.CommitPeriodSeconds))
                            {
                                if (_eventStore.Any())
                                {
                                    lastTransactionCommit = CommitTransaction(consumer);
                                }
                            }
                        }
                        catch (KafkaException e)
                        {
                            var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                            Log($"TESTAPP Consumer KafkaException exception: {e.Message}, TxnRequiresAbort: {txnRequiresAbort}");
                            if (txnRequiresAbort)
                            {
                                Retry("Consumer", () =>
                                {
                                    RewindConsumer(consumer, CommittedTimeout);
                                }, RetryAfterSleeping);
                            }
                        }
                        catch (Exception e)
                        {
                            Log($"TESTAPP Consumer Exception type: {e.GetType()}");
                            if (e is OperationCanceledException)
                            {
                                _isConsumerClosing = true;
                                Log("TESTAPP Closing consumer");
                                consumer.Close();
                                break;
                            }
                            else
                            {
                                Log($"TESTAPP Caught inner exception: {e.Message}");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log($"TESTAPP Caught outer exception: {e.Message}");
            }

            Log($"TESTAPP Process finished");

            _isProcessFinished = true;
        }

        private void Retry(string operation, Action action, Action<string, KafkaException> onKafkaException = null)
        {
            while (!_isProcessFinished)
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
                    Log($"TESTAPP {operation} Caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }
        }

        private DateTime CommitTransaction(IConsumer<string, string> consumer)
        {
            while (!_isProcessFinished)
            {
                try
                {
                    if (recreateProducer)
                    {
                        RecreateProducer();
                        recreateProducer = false;
                    }
                    try
                    {
                        Log("TESTAPP calling BeginTransaction");
                        _producer.BeginTransaction();
                    }
                    catch (Exception e)
                    {
                        Log($"TESTAPP BeginTransaction threw: {e.Message}");
                        throw;
                    }

                    Log($"TESTAPP Transaction contains {_eventStore.Count} messages");

                    stopwatch.Reset();
                    stopwatch.Start();
                    Retry("Produce", () =>
                    {
                        Log("TESTAPP calling Produce many times");
                        foreach (var @event in _eventStore)
                        {
                            _producer.Produce(ProcessConfig.OutputTopic, new Message<string, string> { Key = @event.Key, Value = @event.Value });
                        }
                    }, RetryUnlessFatalOrAborted);

                    Log($"TESTAPP Produce messages elapsed: {stopwatch.Elapsed}");

                    stopwatch.Reset();
                    stopwatch.Start();
                    Retry("SendOffsetsToTransaction", () =>
                    {
                        Log("TESTAPP calling SendOffsetsToTransaction");
                        _producer.SendOffsetsToTransaction(
                            consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                            consumer.ConsumerGroupMetadata,
                            TransactionTimeout);
                    }, RetryIfNeeded);

                    stopwatch.Stop();
                    Log($"TESTAPP SendOffsetsToTransaction elapsed: {stopwatch.Elapsed}");

                    stopwatch.Reset();
                    stopwatch.Start();
                    Retry("CommitTransaction", () =>
                    {
                        Log("TESTAPP calling CommitTransaction");
                        _producer.CommitTransaction();
                    }, RetryIfNeeded);

                    stopwatch.Stop();
                    Log($"TESTAPP CommitTransaction elapsed: {stopwatch.Elapsed}");

                    _eventStore.Clear();

                    break;
                }
                catch (KafkaException e)
                {
                    Log($"TESTAPP Kafka Exception caught, aborting transaction, trying again in {RetryAfterMs.TotalSeconds} seconds: '{e.Message}'");
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        recreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            Log("TESTAPP calling AbortTransaction");
                            _producer.AbortTransaction(TransactionTimeout);
                        }, (string operation, KafkaException eInner) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            Log($"TESTAPP AbortTransaction Kafka Exception caught, trying again in {RetryAfterMs.TotalSeconds} seconds if not fatal: '{eInner.Message}', IsFatal: {IsFatalInner}, TxnRequiresAbort: {TxnRequiresAbortErrorInner}, IsRetriable: {IsRetriableInner}");
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    recreateProducer = true;
                                }
                                // Propagate abort to consumer
                                throw e;
                            }
                            RetryAfterSleeping();
                        });
                        // Propagate abort to consumer
                        throw;
                    }
                    RetryAfterSleeping();
                }
                catch
                {
                    Log("TESTAPP Caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }

            DateTime lastTransactionCommit = DateTime.UtcNow;
            return lastTransactionCommit;
        }


        private void PartitionsRevokedHandler(IConsumer<string, string> consumer, List<TopicPartitionOffset> partitions)
        {
            Log("TESTAPP Partitions revoked.");
            if (!_isConsumerClosing)
            {
                CommitTransaction(consumer);
            }
        }

        private void PartitionsLostHandler(IConsumer<string, string> consumer, List<TopicPartitionOffset> partitions)
        {
            Log("TESTAPP Partitions lost.");
        }

        private void Log(string message)
        {
            double ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, 0)).TotalSeconds;
            Console.Error.WriteLine(String.Format("{0:0.000}%", ts) + ": " + message);
        }
    }
}
