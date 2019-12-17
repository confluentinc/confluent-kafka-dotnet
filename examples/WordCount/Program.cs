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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using RocksDbSharp;


namespace Confluent.Kafka.Examples.WordCount
{
    public class Program
    {
        /// <summary>
        ///     Default timeout used for transaction related operations.
        /// </summary>
        static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        ///     The numer of partitions we'll use for each topic.
        /// </summary>
        const int NumPartitions = 12;

        /// <summary>
        ///     The name of the Kafka topic containing the input lines of text to count the words in.
        ///         * key = null
        ///         * value = a line of text (type: string)
        /// </summary>
        const string Topic_InputLines = "line-input";

        /// <summary>
        ///     The name of the Kafka topic containing the stream of words to count.
        ///         * key = individual words (type: string)
        ///         * value = null
        /// </summary>
        const string Topic_Words = "words";

        /// <summary>
        ///     The name of the Kafka topic containing the word count state [should be compacted].
        ///         * key = word (type: string)
        ///         * value = count (type: int)
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
            using (var producer = new ProducerBuilder<string, Null>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<Null, string>(cConfig)
                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** MapWords consumer group rebalanced. Partition assignment: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");
                    Console.WriteLine("Aborting current MapWords transaction.");
                    producer.AbortTransaction(DefaultTimeout);
                    c.Committed(c.Assignment, DefaultTimeout).ForEach(tpo => c.Seek(tpo));
                    producer.BeginTransaction();
                })
                .Build())
            {
                consumer.Subscribe(Topic_InputLines);
                producer.InitTransactions(DefaultTimeout);

                producer.BeginTransaction();
                var offsets = new Dictionary<TopicPartition, Offset>();
                var wCount = 0;
                var lCount = 0;
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(ct);
                        lCount += 1;

                        offsets[cr.TopicPartition] = cr.Offset;

                        var words = Regex.Split(cr.Value.ToLower(), @"[^a-zA-Z_]").Where(s => s != String.Empty);
                        foreach (var w in words)
                        {
                            // todo: handle queue full exception.
                            producer.Produce(Topic_Words, new Message<string, Null> { Key = w });
                            wCount += 1;
                        }

                        if (DateTime.Now > lastTxnCommit + TxnCommitPeriod)
                        {
                            lastTxnCommit = DateTime.Now;
                            producer.SendOffsetsToTransaction(offsets.Select(a => new TopicPartitionOffset(a.Key, a.Value)), ConsumerGroup_MapWords, DefaultTimeout);
                            offsets.Clear();
                            producer.CommitTransaction(DefaultTimeout);
                            Console.WriteLine($"Committed MapWords transaction comprising {wCount} words from {lCount} lines.");
                            wCount = 0;
                            lCount = 0;
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
                        producer.AbortTransaction(DefaultTimeout);
                        consumer.Committed(consumer.Assignment, DefaultTimeout).ForEach(tpo => consumer.Seek(tpo));
                    }
                }
            }
        }

        
        /// <summary>
        ///     Materialize the count state in the Topic_Counts change log topic for the
        ///     specified partitions into a Dictionary&lt;string, int&gt;
        /// </summary>
        public static void LoadCountState(RocksDb db, string brokerList, IEnumerable<Partition> partitions, ColumnFamilyHandle columnFamily, CancellationToken ct)
        {
            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_LoadState,
                EnablePartitionEof = true
            };

            int msgCount = 0;
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
                        msgCount += 1;
                        db.Put(Encoding.UTF8.GetBytes(cr.Key), BitConverter.GetBytes(cr.Value), columnFamily);
                    }
                }
            }

            Console.WriteLine($"Finished materializing word counts state. Backed by {msgCount} messages in Kafka");
        }


        /// <summary>
        ///     A transactional (exactly once) processing loop that reads individual words and updates 
        ///     the corresponding total count state.
        ///
        ///     When a rebalance occurs (including on startup), the total count state for all assigned
        ///     partitions is reloaded before the loop comences to update it.
        /// </summary>
        public static void Processor_AggregateWords(string brokerList, string clientId, RocksDb db, CancellationToken ct)
        {
            var TxnCommitPeriod = TimeSpan.FromSeconds(10);

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

            var writeBatch = new WriteBatch();
            ColumnFamilyHandle columnFamily = null;
            var lastTxnCommit = DateTime.Now;
            using (var producer = new ProducerBuilder<string, int>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<string, Null>(cConfig)
                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** AggregateWords consumer group rebalanced. Partition assignment: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    // clear rocksdb state.
                    db.DropColumnFamily("counts");
                    db.CreateColumnFamily(new ColumnFamilyOptions(), "counts");
                    
                    Console.WriteLine("Aborting current AggregateWords transaction.");
                    producer.AbortTransaction(DefaultTimeout);
                    c.Committed(c.Assignment, DefaultTimeout).ForEach(tpo => c.Seek(tpo));
                    producer.BeginTransaction();
                    writeBatch.Clear();

                    columnFamily = db.GetColumnFamily("counts");
                    LoadCountState(db, brokerList, partitions.Select(p => p.Partition), columnFamily, ct);
                })
                .Build())
            {
                consumer.Subscribe(Topic_Words);
                producer.InitTransactions(DefaultTimeout);

                producer.BeginTransaction();
                var offsets = new Dictionary<TopicPartition, Offset>();
                var wCount = 0;

                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(ct);
                        offsets[cr.TopicPartition] = cr.Offset;

                        var kBytes = Encoding.UTF8.GetBytes(cr.Key);
                        var vBytes = db.Get(kBytes, columnFamily);
                        var v = vBytes == null ? 0 : BitConverter.ToInt32(vBytes);
                        var updatedV = v+1;

                        writeBatch.Put(kBytes, BitConverter.GetBytes(updatedV), columnFamily);

                        while (true)
                        {
                            try
                            {
                                // It's acceptable to omit checking delivery results (delivered to the
                                // application via the optional callback passed to produce) for errors
                                // because any problem they expose will also be covered by the call to
                                // commit transactions.
                                producer.Produce(Topic_Counts, new Message<string, int> { Key = cr.Key, Value = updatedV });
                            }
                            catch (KafkaException e)
                            {
                                // An immediate failure of the produce call is most often caused by the
                                // local message queue being full, and appropriate response to that is 
                                // to retry. Else: todo, good explanation.
                                if (e.Error.Code == ErrorCode.Local_QueueFull)
                                {
                                    Thread.Sleep(TimeSpan.FromSeconds(1000));
                                    continue;
                                }
                                throw;
                            }
                            break;
                        }

                        wCount += 1;

                        if (DateTime.Now > lastTxnCommit + TxnCommitPeriod)
                        {
                            lastTxnCommit = DateTime.Now;
                            db.Write(writeBatch);
                            writeBatch.Clear();
                            producer.SendOffsetsToTransaction(offsets.Select(a => new TopicPartitionOffset(a.Key, a.Value)), ConsumerGroup_Aggregate, DefaultTimeout);
                            offsets.Clear();
                            producer.CommitTransaction(DefaultTimeout);
                            Console.WriteLine($"Committed AggregateWords transaction comprising updates to {wCount} words.");
                            wCount = 0;
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
                        if (e.Error.IsFatal) { throw; }

                        try
                        {
                            producer.AbortTransaction(DefaultTimeout);
                            consumer.Committed(consumer.Assignment, DefaultTimeout).ForEach(tpo => consumer.Seek(tpo));
                        }
                        catch
                        {
                            if (e.Error.IsFatal) { throw; }
                            // TODO: unsure what is most appropriate here.
                        }
                    }
                }
            }
        }

        public async static Task PeriodicallyDisplayTopCountsState(string brokerList, RocksDb db, CancellationToken ct)
        {
            while (true)
            {
                await Task.Delay(10000, ct);
                var it = db.NewIterator(db.GetColumnFamily("counts")).SeekToFirst();

                var N = 5;
                var maxWords = new List<(int, string)>();
                while (it.Valid())
                {
                    var wc = (BitConverter.ToInt32(it.Value()), Encoding.UTF8.GetString(it.Key()));
                    if (maxWords.Count < N) { maxWords.Add(wc); }
                    else { if (wc.Item1 > maxWords[N-1].Item1) { maxWords[N-1] = wc; } }
                    maxWords.Sort((x, y) => y.Item1.CompareTo(x.Item1));
                    it.Next();
                }

                if (maxWords.Count > 0) { Console.WriteLine("Most frequently occuring words known to this instance:"); }
                foreach (var wc in maxWords)
                {
                    Console.WriteLine(" " + wc.Item2 + " " + wc.Item1);
                }
            }
        }

        public static async Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. brokerList [gen|proc] client-id");
                return;
            }

            string brokerList = args[0];
            string mode = args[1];
            string clientId = args[2];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            if (mode == "gen")
            {
                await Generator_LineInputData(brokerList, cts.Token);
                return;
            }

            RocksDb db = null;
            try
            {
                var options = new DbOptions()
                    .SetCreateIfMissing(true)
                    .SetCreateMissingColumnFamilies(true);

                var columnFamilies = new ColumnFamilies { { "counts", new ColumnFamilyOptions() } };

                db = RocksDb.Open(options, System.IO.Path.Join(System.IO.Path.GetTempPath(), "rocksdb_" + Guid.NewGuid().ToString()), columnFamilies);

                await CreateTopicsMaybe(brokerList);
                var processors = new List<Task>();
                processors.Add(Task.Run(() => Processor_MapWords(brokerList, clientId, cts.Token)));
                processors.Add(Task.Run(() => Processor_AggregateWords(brokerList, clientId, db, cts.Token)));
                processors.Add(Task.Run(async () => await PeriodicallyDisplayTopCountsState(brokerList, db, cts.Token)));

                var allTasks = await Task.WhenAny(processors.ToArray());
                if (allTasks.IsFaulted)
                {
                    Console.WriteLine(allTasks.Exception.InnerException.ToString());
                }
                
                Console.WriteLine("exiting...");
            }
            finally
            {
                if (db != null) { db.Dispose(); }
            }
        }
    }
}
