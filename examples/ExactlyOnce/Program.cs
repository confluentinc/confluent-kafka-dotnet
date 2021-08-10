// Copyright 2021 Confluent Inc.
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
using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using FASTER.core;


/// <summary>
///     This example demonstrates how to count the number of occurrences of each word in a
///     given input set, where the input set can potentially be large enough that the
///     computation needs to be distributed across many machines. This is an example of a
///     streaming [map-reduce](https://en.wikipedia.org/wiki/MapReduce) calculation, with
///     **exactly once processing** semantics.
///
///     There are 3 stages:
///
///     gen:     Generates line input data.
///     map:     Extracts words from the line data and partitions them into a new topic.
///     reduce:  Counts the total number of each word.
///
///     The reduce phase uses FASTER to materialize word count state backed by a Kafka
///     topic. FASTER is a local KV store by Microsoft that is reportedly more performant
///     than RocksDb.
///
///     https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
///     https://github.com/microsoft/FASTER
///
///     If you know that the working state will always be less than available memory, you do
///     not need to use FASTER, or RocksDb or similar - an in memory collection such as
///     Dictionary<,> will be easier and more performant.
/// </summary>
namespace Confluent.Kafka.Examples.ExactlyOnce
{
    public class Program
    {
        /// <summary>
        ///     Default timeout used for transaction related operations.
        /// </summary>
        static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        ///     The number of partitions we'll use for each topic.
        /// </summary>
        const int NumPartitions = 12;

        /// <summary>
        ///     The name of the input Kafka topic containing the lines of text to count the words in.
        ///         * key = null
        ///         * value = a line of text (type: string)
        /// </summary>
        const string Topic_InputLines = "lines";

        /// <summary>
        ///     The name of the Kafka topic containing the stream of words to count.
        ///         * key = individual words (type: string)
        ///         * value = null.
        ///
        ///     Note: This topic is not compacted, so setting the value to null is appropriate. Be
        ///           aware that for compacted topics though, null has special meaning - it's a
        ///           tombstone marker.
        /// </summary>
        const string Topic_Words = "words";

        /// <summary>
        ///     The name of the Kafka topic containing the word count state (should be compacted).
        ///         * key = word (type: string)
        ///         * value = count (type: int)
        /// </summary>
        const string Topic_Counts = "counts";


        /// <summary>
        ///     The transactional id stem for the word splitting processor.
        /// </summary>
        const string TransactionalIdPrefix_MapWords = "map-words-transaction-id";


        /// <summary>
        ///     The transactional id stem for the word count processor.
        /// </summary>
        const string TransactionalIdPrefix_Aggregate = "aggregator-transaction-id";


        /// <summary>
        ///     Name of the consumer group used in the map phase when consuming
        ///     from the Topic_InputLines topic.
        /// </summary>
        const string ConsumerGroup_MapWords = "map-words-consumer-group";

        /// <summary>
        ///     Name of the consumer group used in the reduce phase when
        ///     consuming from the Topic_Words topic.
        /// </summary>
        const string ConsumerGroup_Aggregate = "aggregate-consumer-group";

        /// <summary>
        ///     Name of the consumer group used when loading count state
        ///     from Topic_Counts.
        /// </summary>
        const string ConsumerGroup_LoadState = "load-state-consumer-group";


        class FasterState : IDisposable
        {
            public FasterState(Partition partition)
            {
                this.Path = System.IO.Path.Join(System.IO.Path.GetTempPath(), "partition_" + (int)partition);
                this.ObjectLog = Devices.CreateLogDevice(this.Path + "_obj.log");
                this.Log = Devices.CreateLogDevice(this.Path + ".log");
                this.Store = new FasterKV<string, int>(
                    size: 1L << 20, // 1M cache lines of 64 bytes each = 64MB hash table
                    logSettings: new LogSettings { LogDevice = this.Log, ObjectLogDevice = this.ObjectLog }
                );
                var funcs = new SimpleFunctions<string, int>((a, b) => a + b); // function used for read-modify-write (RMW).
                this.Session = this.Store.NewSession(funcs);
            }

            string Path { get; set; }
            IDevice Log { get; set; }
            IDevice ObjectLog { get; set; }
            public FasterKV<string, int> Store { get; set; }
            public ClientSession<string, int, int, int, Empty, IFunctions<string, int, int, int, Empty>> Session { get; set; }

            public void Dispose()
            {
                this.Session.Dispose();
                this.Store.Dispose();
                this.Log.Dispose();
                this.ObjectLog.Dispose();
                File.Delete(this.Path);
            }
        }

        /// <summary>
        ///     Cache of materialized word count state corresponding to partitions currently
        ///     assigned to this instance.
        /// </summary>
        static Dictionary<Partition, FasterState> WordCountState = new Dictionary<Partition, FasterState>();

        /// <summary>
        ///     Remove all topics used by this example if they exist.
        /// </summary>
        static async Task DeleteTopics(string brokerList, string clientId)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId
            };

            using (var adminClent = new AdminClientBuilder(config).Build())
            {
                try
                {
                    await adminClent.DeleteTopicsAsync(new List<string> { Topic_InputLines, Topic_Words, Topic_Counts });
                }
                catch (DeleteTopicsException e)
                {
                    // propagate the exception unless the error was that one or more of the topics didn't exist.
                    if (e.Results.Select(r => r.Error.Code).Where(el => el != ErrorCode.UnknownTopicOrPart && el != ErrorCode.NoError).Count() > 0)
                    {
                        throw new Exception("Unable to delete topics", e);
                    }
                }
            }
        }

        /// <summary>
        ///     Create all topics used by this example, if they don't already exist.
        /// </summary>
        static async Task CreateTopicsMaybe(string brokerList, string clientId)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId
            };

            using (var adminClent = new AdminClientBuilder(config).Build())
            {
                var countsTopicSpec = new TopicSpecification
                {
                    Name = Topic_Counts,
                    // note: in production, you should generally use a replication factor of 3.
                    ReplicationFactor = 1,
                    NumPartitions = NumPartitions,
                    // this topic backs a kv (word -> count) state store, so it can be compacted.
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
                    // propagate the exception unless the error was that one or more of the topics already exists.
                    if (ex.Results.Select(r => r.Error.Code).Where(el => el != ErrorCode.TopicAlreadyExists && el != ErrorCode.NoError).Count() > 0)
                    {
                        throw new Exception("Unable to create topics", ex);
                    }
                }
            }
        }


        /// <summary>
        ///     Generate example line input data (using the source code of this example as the source!).
        /// </summary>
        static async Task Generator_LineInputData(string brokerList, string clientId, CancellationToken ct)
        {
            var client = new HttpClient();
            var r = await client.GetAsync("https://raw.githubusercontent.com/confluentinc/confluent-kafka-dotnet/master/examples/ExactlyOnce/Program.cs", ct);
            r.EnsureSuccessStatusCode();
            var content = await r.Content.ReadAsStringAsync();
            var lines = content.Split('\n');

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId
            };

            Console.WriteLine($"Producing text line data to topic: {Topic_InputLines}");
            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                var lCount = 0;
                foreach (var l in lines)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);  // slow down the calls to produce to make the output more interesting to watch.
                    await producer.ProduceAsync(Topic_InputLines, new Message<Null, string> { Value = l }, ct);  // Note: producing synchronously is slow and should generally be avoided.
                    lCount += 1;
                    if (lCount % 10 == 0)
                    {
                        Console.WriteLine($"Produced {lCount} input lines.");
                    }
                }

                producer.Flush(ct);
            }

            Console.WriteLine("Generator_LineInputData: Wrote all input lines to Kafka");
        }


        /// <summary>
        ///     A transactional (exactly once) processing loop that reads lines of text from
        ///     Topic_InputLines, splits them into words, and outputs the result to Topic_Words.
        /// </summary>
        static void Processor_MapWords(string brokerList, string clientId, CancellationToken ct)
        {
            if (clientId == null)
            {
                throw new Exception("Map processor requires that a client id is specified.");
            }

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_producer",
                // The TransactionalId identifies this instance of the map words processor.
                // If you start another instance with the same transactional id, the existing
                // instance will be fenced.
                TransactionalId = TransactionalIdPrefix_MapWords + "-" + clientId
            };

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_consumer",
                GroupId = ConsumerGroup_MapWords,
                // AutoOffsetReset specifies the action to take when there
                // are no committed offsets for a partition, or an error
                // occurs retrieving offsets. If there are committed offsets,
                // it has no effect.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Offsets are committed using the producer as part of the
                // transaction - not the consumer. When using transactions,
                // you must turn off auto commit on the consumer, which is
                // enabled by default!
                EnableAutoCommit = false,
                // Enable incremental rebalancing by using the CooperativeSticky
                // assignor (avoid stop-the-world rebalances).
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            var txnCommitPeriod = TimeSpan.FromSeconds(10);

            var lastTxnCommit = DateTime.Now;

            using (var producer = new ProducerBuilder<string, Null>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<Null, string>(cConfig)
                .SetPartitionsRevokedHandler((c, partitions) => {
                    var remaining = c.Assignment.Where(tp => partitions.Where(x => x.TopicPartition == tp).Count() == 0);
                    Console.WriteLine(
                        "** MapWords consumer group partitions revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");

                    // All handlers (except the log handler) are executed as a
                    // side-effect of, and on the same thread as the Consume or
                    // Close methods. Any exception thrown in a handler (with
                    // the exception of the log and error handlers) will
                    // be propagated to the application via the initiating
                    // call. i.e. in this example, any exceptions thrown in this
                    // handler will be exposed via the Consume method in the main
                    // consume loop and handled by the try/catch block there.

                    producer.SendOffsetsToTransaction(
                        c.Assignment.Select(a => new TopicPartitionOffset(a, c.Position(a))),
                        c.ConsumerGroupMetadata,
                        DefaultTimeout);
                    producer.CommitTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsLostHandler((c, partitions) => {
                    // Ownership of the partitions has been involuntarily lost and
                    // are now likely already owned by another consumer.

                    Console.WriteLine(
                        "** MapWords consumer group partitions lost: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    producer.AbortTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** MapWords consumer group additional partitions assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // No action is required here related to transactions - offsets
                    // for the newly assigned partitions will be committed in the
                    // main consume loop along with those for already assigned
                    // partitions as per usual.
                })
                .Build())
            {
                consumer.Subscribe(Topic_InputLines);

                producer.InitTransactions(DefaultTimeout);
                producer.BeginTransaction();

                var wCount = 0;
                var lCount = 0;
                while (true)
                {
                    try
                    {
                        ct.ThrowIfCancellationRequested();

                        // Do not block on Consume indefinitely to avoid the possibility of a transaction timeout.
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (cr != null)
                        {
                            lCount += 1;

                            var words = Regex.Split(cr.Message.Value.ToLower(), @"[^a-zA-Z_]").Where(s => s != String.Empty);
                            foreach (var w in words)
                            {
                                while (true)
                                {
                                    try
                                    {
                                        producer.Produce(Topic_Words, new Message<string, Null> { Key = w });
                                        // Note: when using transactions, there is no need to check for errors of individual
                                        // produce call delivery reports because if the transaction commits successfully, you
                                        // can be sure that all the constituent messages were delivered successfully and in order.

                                        wCount += 1;
                                    }
                                    catch (KafkaException e)
                                    {
                                        // An immediate failure of the produce call is most often caused by the
                                        // local message queue being full, and appropriate response to that is
                                        // to wait a bit and retry.
                                        if (e.Error.Code == ErrorCode.Local_QueueFull)
                                        {
                                            Thread.Sleep(TimeSpan.FromMilliseconds(1000));
                                            continue;
                                        }
                                        throw;
                                    }
                                    break;
                                }
                            }
                        }

                        // Commit transactions every TxnCommitPeriod
                        if (DateTime.Now > lastTxnCommit + txnCommitPeriod)
                        {
                            // Note: Exceptions thrown by SendOffsetsToTransaction and
                            // CommitTransaction that are not marked as fatal can be
                            // recovered from. However, in order to keep this example
                            // short(er), the additional logic required to achieve this
                            // has been omitted. This should happen only rarely, so
                            // requiring a process restart in this case is not necessarily
                            // a bad compromise, even in production scenarios.

                            producer.SendOffsetsToTransaction(
                                // Note: committed offsets reflect the next message to consume, not last
                                // message consumed. consumer.Position returns the last consumed offset
                                // values + 1, as required.
                                consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                consumer.ConsumerGroupMetadata,
                                DefaultTimeout);
                            producer.CommitTransaction();
                            producer.BeginTransaction();

                            Console.WriteLine($"Committed MapWords transaction(s) comprising {wCount} words from {lCount} lines.");
                            lastTxnCommit = DateTime.Now;
                            wCount = 0;
                            lCount = 0;
                        }
                    }
                    catch (Exception e)
                    {
                        // Attempt to abort the transaction (but ignore any errors) as a measure
                        // against stalling consumption of Topic_Words.
                        producer.AbortTransaction();

                        Console.WriteLine("Exiting MapWords consume loop due to an exception: " + e);
                        // Note: transactions may be committed / aborted in the partitions
                        // revoked / lost handler as a side effect of the call to close.
                        consumer.Close();
                        Console.WriteLine("MapWords consumer closed");
                        break;
                    }

                    // Assume the presence of an external system that monitors whether worker
                    // processes have died, and restarts new instances as required. This
                    // setup is typical, and avoids complex error handling logic in the
                    // client code.
                }
            }
        }


        /// <summary>
        ///     Materialize the count state in the Topic_Counts change log topic for the
        ///     specified partitions into FASTER KV stores.
        /// </summary>
        public static void LoadCountState(string brokerList, IEnumerable<Partition> partitions, CancellationToken ct)
        {
            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_LoadState,
                EnablePartitionEof = true,
                EnableAutoCommit = false
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
                        WordCountState[cr.Partition].Session.Upsert(cr.Message.Key, cr.Message.Value);
                    }
                }
            }

            Console.WriteLine($"Finished materializing word counts state ({msgCount} messages) for partitions [" +
                string.Join(',', partitions.Select(p => p.Value)) + $"] in Kafka topic '{Topic_Counts}'. ");
        }


        /// <summary>
        ///     A transactional (exactly once) processing loop that reads individual words and updates 
        ///     the corresponding total count state.
        ///
        ///     When a rebalance occurs (including on startup), the count state for the incrementally
        ///     assigned partitions is reloaded before the loop commences to update it. For this use-
        ///     case, the CooperativeSticky assignor is much more efficient than the Range or RoundRobin
        ///     assignors since it keeps to a minimum the count state that needs to be materialized.
        /// </summary>
        /// <remarks>
        ///     Refer to Processor_MapWords for more detailed comments.
        /// </remarks>
        public static void Processor_AggregateWords(string brokerList, string clientId, CancellationToken ct)
        {
            if (clientId == null)
            {
                throw new Exception("Aggregate words processor requires that a client id is specified.");
            }

            var txnCommitPeriod = TimeSpan.FromSeconds(10);

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_producer",
                TransactionalId = TransactionalIdPrefix_Aggregate + "-" + clientId
            };

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_consumer",
                GroupId = ConsumerGroup_Aggregate,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // This should be greater than the maximum amount of time required to read in
                // existing count state. It should not be too large, since a rebalance may be
                // blocked for this long.
                MaxPollIntervalMs = 600000, // 10 minutes.
                EnableAutoCommit = false,
                // Enable incremental rebalancing by using the CooperativeSticky
                // assignor (avoid stop-the-world rebalances). This is particularly important,
                // in the AggregateWords case, since the entire count state for newly assigned
                // partitions is loaded in the partitions assigned handler.
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            };

            var lastTxnCommit = DateTime.Now;

            using (var producer = new ProducerBuilder<string, int>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<string, Null>(cConfig)
                .SetPartitionsRevokedHandler((c, partitions) => {
                    var remaining = c.Assignment.Where(tp => partitions.Where(x => x.TopicPartition == tp).Count() == 0);
                    Console.WriteLine(
                        "** AggregateWords consumer group partitions revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");

                    // Remove materialized word count state for the partitions that have been revoked.
                    foreach (var tp in partitions)
                    {
                        WordCountState[tp.Partition].Dispose();
                        WordCountState.Remove(tp.Partition);
                    }

                    producer.SendOffsetsToTransaction(
                        c.Assignment.Select(a => new TopicPartitionOffset(a, c.Position(a))),
                        c.ConsumerGroupMetadata,
                        DefaultTimeout);
                    producer.CommitTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsLostHandler((c, partitions) => {
                    Console.WriteLine(
                        "** AggregateWords consumer group partitions lost: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    // clear materialized word count state for all partitions.
                    foreach (var tp in partitions)
                    {
                        WordCountState[tp.Partition].Dispose();
                        WordCountState.Remove(tp.Partition);
                    }

                    producer.AbortTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** AggregateWords consumer group partition assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    if (partitions.Count > 0)
                    {
                        // Initialize FASTER KV stores for each new partition.
                        foreach (var tp in partitions)
                        {
                            var partitionState = new FasterState(tp.Partition);
                            WordCountState.Add(tp.Partition, partitionState);
                        }

                        // Materialize count state for partitions into the FASTER KV stores.
                        // Note: the partiioning of Topic_Counts matches Topic_Words.
                        LoadCountState(brokerList, partitions.Select(tp => tp.Partition), ct);
                    }
                })
                .Build())
            {
                consumer.Subscribe(Topic_Words);

                producer.InitTransactions(DefaultTimeout);
                producer.BeginTransaction();

                var wCount = 0;

                while (true)
                {
                    try
                    {
                        ct.ThrowIfCancellationRequested();

                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (cr != null)
                        {
                            string key = cr.Message.Key;
                            var (_, count) = WordCountState[cr.Partition].Session.Read(cr.Message.Key);
                            count += 1;
                            WordCountState[cr.Partition].Session.Upsert(key, count);

                            producer.Produce(Topic_Counts, new Message<string, int> { Key = cr.Message.Key, Value = count });

                            wCount += 1;
                        }

                        if (DateTime.Now > lastTxnCommit + txnCommitPeriod)
                        {
                            producer.SendOffsetsToTransaction(
                                // Note: committed offsets reflect the next message to consume, not last
                                // message consumed. consumer.Position returns the last consumed offset
                                // values + 1, as required.
                                consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                consumer.ConsumerGroupMetadata,
                                DefaultTimeout);
                            producer.CommitTransaction();

                            producer.BeginTransaction();

                            Console.WriteLine($"Committed AggregateWords transaction(s) comprising updates to {wCount} words.");
                            lastTxnCommit = DateTime.Now;
                            wCount = 0;
                        }
                    }
                    catch (Exception e)
                    {
                        producer.AbortTransaction();

                        Console.WriteLine("Exiting AggregateWords consume loop due to an exception: " + e);
                        consumer.Close();
                        Console.WriteLine("AggregateWords consumer closed");
                        break;
                    }
                }
            }
        }

        public async static Task PeriodicallyDisplayTopCountsState(string brokerList, CancellationToken ct)
        {
            while (true)
            {
                await Task.Delay(10000, ct);

                var N = 5;
                var maxWords = new List<(int, string)>();
                foreach (var kvp in WordCountState)
                {
                    var store = kvp.Value;
                    var itr = store.Store.Iterate();

                    while(itr.GetNext(out var recordInfo))
                    {
                        var wc = (itr.GetValue(), itr.GetKey());
                        if (maxWords.Count < N) { maxWords.Add(wc); }
                        else { if (wc.Item1 > maxWords[N-1].Item1) { maxWords[N-1] = wc; } }
                        maxWords.Sort((x, y) => y.Item1.CompareTo(x.Item1));
                    }
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
            Action printUsage = () =>
            {
                Console.WriteLine("Usage: .. <brokerList> gen|del|map|count [client-id]");
                Console.WriteLine("   del:     delete all Kafka topics created by this application (client-id optional)");
                Console.WriteLine("   gen:     generate line data (client-id optional)");
                Console.WriteLine("   map:     split the input lines into words (client-id required)");
                Console.WriteLine("   reduce:  count the number of occurances of each word (client-id required)");
            };

            if (args.Length != 2 && args.Length != 3)
            {
                printUsage();
                return;
            }

            string brokerList = args[0];
            string mode = args[1];
            string clientId = args.Length > 2 ? args[2] : null;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            if (mode != "del")
            {
                await CreateTopicsMaybe(brokerList, clientId);
            }

            switch (mode)
            {
                case "del":
                    await DeleteTopics(brokerList, clientId);
                    return;

                case "gen":
                    try { await Generator_LineInputData(brokerList, clientId, cts.Token); }
                    catch (OperationCanceledException) {}
                    return;

                case "map":
                    try { Processor_MapWords(brokerList, clientId, cts.Token); }
                    catch (OperationCanceledException) {}
                    return;

                case "reduce":
                    var processors = new List<Task>();
                    processors.Add(Task.Run(() => Processor_AggregateWords(brokerList, clientId, cts.Token)));
                    processors.Add(Task.Run(async () => await PeriodicallyDisplayTopCountsState(brokerList, cts.Token)));

                    var allTasks = await Task.WhenAny(processors.ToArray());
                    if (allTasks.IsFaulted)
                    {
                        if (!(allTasks.Exception.InnerException is OperationCanceledException))
                        {
                            Console.WriteLine(allTasks.Exception.InnerException.ToString());
                        }
                    }

                    Console.WriteLine("exiting...");
                    return;

                default:
                    printUsage();
                    return;
            }

        }
    }
}
