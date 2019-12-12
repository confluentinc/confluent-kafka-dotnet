// Copyright 2019 Confluent Inc.
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Confluent.Kafka.Examples.ProducerExample
{
    public class Program
    {
        /// <summary>
        ///     The numer of partitions we'll use for each topic.
        /// </summary>
        const int NumPartitions = 12;

        /// <summary>
        ///     The name of the Kafka topic containing the input lines of text to count the words in.
        ///         * key = null
        ///         * value = line of text: string
        /// </summary>
        const string Topic_InputLines = "line-input";

        /// <summary>
        ///     The name of the Kafka topic containing the stream of words to count.
        ///         * key = individual words: string
        ///         * value = null
        /// </summary>
        const string Topic_Words = "words";

        /// <summary>
        ///     The name of the Kafka topic containing the word count state [should be compacted].
        ///         * key = word: string
        ///         * value = count: int
        /// </summary>
        const string Topic_Counts = "counts";


        const string TransactionalId_MapWords = "map-words-transaction-id";

        const string TransactionalId_Aggregate = "aggregator-transaction-id";


        const string ConsumerGroup_MapWords = "map-words-consumer-group";

        const string ConsumerGroup_Aggregate = "aggregate-consumer-group";

        const string ConsumerGroup_LoadState = "load-state-consumer-group";


        /// <summary> 
        ///     Create all topics used by this example, if they don't already exist.
        /// </summary>
        static async Task CreateTopicsMaybe(string brokerList)
        {
            var config = new AdminClientConfig { BootstrapServers = brokerList };

            using (var adminClent = new AdminClientBuilder(config).Build())
            {
                var countsTopicSpec = new TopicSpecification
                {
                    Name = Topic_Counts,
                    ReplicationFactor = 1,
                    NumPartitions = NumPartitions,
                    // this topic backs a kv (word -> count) state store, so we can make it compacted.
                    Configs = new Dictionary<string, string> { { "cleanup.policy", "compact" } }
                };
                
                var wordsTopicSpec = new TopicSpecification
                {
                    Name = Topic_Words,
                    ReplicationFactor = 1,
                    NumPartitions = NumPartitions
                };

                var inputLinesTopicSpec = new TopicSpecification
                {
                    Name = Topic_InputLines,
                    ReplicationFactor = 1,
                    NumPartitions = NumPartitions
                };

                try
                {
                    await adminClent.CreateTopicsAsync(new List<TopicSpecification> { countsTopicSpec, wordsTopicSpec, inputLinesTopicSpec });
                }
                catch (CreateTopicsException ex)
                {
                    if (ex.Results.Select(r => r.Error.Code).ToHashSet().SingleOrDefault() != ErrorCode.TopicAlreadyExists)
                    {
                        throw;
                    }
                }
            }
        }


        /// <summary>
        ///     Generate line input data (using the C# consumer source code on github as the source).
        /// </summary>
        static async Task Generator_LineInputData(string brokerList, CancellationToken ct)
        {
            var client = new HttpClient();
            var r = await client.GetAsync("https://raw.githubusercontent.com/confluentinc/confluent-kafka-dotnet/master/src/Confluent.Kafka/Consumer.cs", ct);
            r.EnsureSuccessStatusCode();
            var content = await r.Content.ReadAsStringAsync();
            var lines = content.Split('\n');

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
            };

            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                var lCount = 0;
                foreach (var l in lines)
                {
                    // slow down the produces to make the output more interesting to watch.
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                    await producer.ProduceAsync(Topic_InputLines, new Message<Null, string> { Value = l });
                    lCount += 1;
                    if (lCount % 10 == 0)
                    {
                        Console.WriteLine($"produced {lCount} input lines.");
                    }
                }

                // Note: Ideally we shouldn't block in an async method, but there is no async Flush() method yet, and it's of no consequence here.
                producer.Flush(ct);
            }

            Console.WriteLine("Generator_LineInputData: Wrote all input lines to Kafka");
        }

        /// <summary>
        ///     A transactional (exactly once) processing loop that reads lines of text from Topic_InputLines,
        ///     splits them into words and outputs the result to Topic_Words.
        /// </summary>
        static void Processor_MapWords(string brokerList, string clientId, CancellationToken ct)
        {
            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                EnableIdempotence = true,
                TransactionalId = TransactionalId_MapWords + "-" + clientId
            };

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_MapWords,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var TxnCommitPeriod = TimeSpan.FromSeconds(10);

            var lastTxnCommit = DateTime.Now;
            using (var consumer = new ConsumerBuilder<Null, string>(cConfig).Build())
            using (var producer = new ProducerBuilder<string, Null>(pConfig).Build())
            {
                consumer.Subscribe(Topic_InputLines);
                producer.InitTransactions(TimeSpan.FromSeconds(30));

                producer.BeginTransaction();
                var offsets = new Dictionary<TopicPartition, Offset>();
                var wCount = 0;
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(ct);

                        offsets[cr.TopicPartition] = cr.Offset;

                        var words = Regex.Split(cr.Value.ToLower(), @"[^a-zA-Z0-9_]").Where(s => s != String.Empty);
                        foreach (var w in words)
                        {
                            // todo: handle queue full exception.
                            producer.Produce(Topic_Words, new Message<string, Null> { Key = w });
                            wCount += 1;
                        }

                        if (DateTime.Now > lastTxnCommit + TxnCommitPeriod)
                        {
                            producer.SendOffsetsToTransaction(offsets.Select(a => new TopicPartitionOffset(a.Key, a.Value)), ConsumerGroup_MapWords);
                            offsets.Clear();
                            producer.CommitTransaction(TimeSpan.FromSeconds(30));
                            Console.WriteLine($"Committed MapWords transaction. total {wCount} words prouduced so far.");
                            producer.BeginTransaction();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // try to commit the transaction with a short timeout. If it fails, no problem, it'll abort automatically.
                        producer.CommitTransaction(new TimeSpan(2));
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine("MapWords abortable error: " + e.ToString());
                        if (e.Error.IsFatal)
                        {
                            throw;
                        }

                        // else abortable.
                        producer.AbortTransaction(TimeSpan.FromSeconds(30));
                        consumer.Committed(consumer.Assignment, TimeSpan.FromSeconds(30)).ForEach(tpo => consumer.Seek(tpo));
                    }
                }
            }
        }

        
        /// <summary>
        ///     Materialize the count state in the Topic_Counts change log topic for the
        ///     specified partitions into a Dictionary&lt;string, int&gt;
        /// </summary>
        public static Dictionary<string, int> LoadCountState(string brokerList, IEnumerable<Partition> partitions, CancellationToken ct)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_LoadState,
                EnablePartitionEof = true
            };

            using (var consumer = new ConsumerBuilder<string, int>(cConfig).Build())
            {
                consumer.Assign(partitions.Select(p => new TopicPartitionOffset(Topic_Counts, p, Offset.Beginning)));

                int eofCount = 0;
                while (true)
                {
                    var cr = consumer.Consume();
                    if (cr.IsPartitionEOF)
                    {
                        eofCount += 1;
                        if (eofCount == partitions.Count())
                        {
                            break;
                        }
                    }
                    else
                    {
                        counts[cr.Key] = cr.Value;
                    }
                }
            }

            Console.WriteLine($"Finished materializing count state for {counts.Count} words");
            return counts;
        }

        static Dictionary<string, int> countState = new Dictionary<string, int>();
        static object countLockObj = new object();

        /// <summary>
        ///     A transactional (exactly once) processing loop that reads individual words and updates 
        ///     the corresponding total count state.
        ///
        ///     When a rebalance occurs (including on startup), the total count state for all assigned
        ///     partitions is reloaded before the loop comences to update it.
        /// </summary>
        public static void Processor_AggregateWords(string brokerList, string clientId, CancellationToken ct)
        {
            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                EnableIdempotence = true,
                TransactionalId = TransactionalId_Aggregate + "-" + clientId
            };

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_Aggregate,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // This should be greater than the maximum amount of time required to read in
                // existing count state.
                MaxPollIntervalMs = 86400000
            };

            var TxnCommitPeriod = TimeSpan.FromSeconds(10);

            var lastTxnCommit = DateTime.Now;
            using (var producer = new ProducerBuilder<string, int>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<string, Null>(cConfig)
                .SetPartitionsAssignedHandler((c, partitions) => {
                    var cs = LoadCountState(brokerList, partitions.Select(p => p.Partition), ct);
                    lock (countLockObj)
                    {
                        countState = cs;
                    }
                })
                .Build())
            {
                consumer.Subscribe(Topic_Words);
                producer.InitTransactions(TimeSpan.FromSeconds(30));

                producer.BeginTransaction();
                var offsets = new Dictionary<TopicPartition, Offset>();
                var wCount = 0;
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(ct);
                        offsets[cr.TopicPartition] = cr.Offset;

                        lock (countLockObj)
                        {
                            if (!countState.ContainsKey(cr.Key)) { countState.Add(cr.Key, 0); }
                            countState[cr.Key] += 1;

                            // TODO: handle queue full exception.
                            producer.Produce(Topic_Counts, new Message<string, int> { Key = cr.Key, Value = countState[cr.Key] });
                            wCount += 1;
                        }

                        if (DateTime.Now > lastTxnCommit + TxnCommitPeriod)
                        {
                            producer.SendOffsetsToTransaction(offsets.Select(a => new TopicPartitionOffset(a.Key, a.Value)), ConsumerGroup_Aggregate);
                            offsets.Clear();
                            producer.CommitTransaction(TimeSpan.FromSeconds(30));
                            Console.WriteLine($"Committed word count txn. total words counted so far: {wCount}");
                            producer.BeginTransaction();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // try to commit the transaction with a short timeout. If it fails, no problem, it'll abort automatically.
                        producer.CommitTransaction(new TimeSpan(2));
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine("AggregateWords error: " + e.ToString());
                        if (e.Error.IsFatal)
                        {
                            throw;
                        }

                        // else abortable.
                        producer.AbortTransaction(TimeSpan.FromSeconds(30));
                        consumer.Committed(consumer.Assignment, TimeSpan.FromSeconds(30)).ForEach(tpo => consumer.Seek(tpo));
                    }
                }
            }
        }

        public async static Task PeriodicallyDisplayTopCountsState(string brokerList)
        {
            while (true)
            {
                await Task.Delay(5000);
                lock (countLockObj)
                {
                    var countCount = 0;
                    foreach (var oc in countState.OrderBy(a => a.Value).Reverse())
                    {
                        if (countCount++ > 3) { break; }
                        Console.WriteLine(oc.Key + ": " + oc.Value);
                    }
                }
                Console.WriteLine("---\n");
            }
        }

        public static async Task Main(string[] args)
        {
            Confluent.Kafka.Library.Load("/usr/local/lib/librdkafka.so");

            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList client-id");
                return;
            }

            string brokerList = args[0];
            string clientId = args[1];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                await CreateTopicsMaybe(brokerList);
                var processors = new List<Task>();
                processors.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await Generator_LineInputData(brokerList, cts.Token);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("LineInputData failed: " + e.ToString());
                        }
                    }));
                processors.Add(Task.Run(() =>
                    {
                        try
                        {
                            Processor_MapWords(brokerList, clientId, cts.Token);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("MapWords failed: " + e.ToString());
                        }
                    }));
                processors.Add(Task.Run(() =>
                    {
                        try
                        {
                            Processor_AggregateWords(brokerList, clientId, cts.Token);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Aggregate failed: " + e.ToString());
                        }
                    }));
                processors.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await PeriodicallyDisplayTopCountsState(brokerList);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Count state display failed: " + e.ToString());
                        }
                    }));

                await Task.WhenAny(processors.ToArray());
                Console.WriteLine("exiting");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nApplication terminated.");
            }
        }
    }
}
