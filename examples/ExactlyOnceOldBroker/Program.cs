// Copyright 2020 Confluent Inc.
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
using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using RocksDbSharp;


namespace Confluent.Kafka.Examples.Transactions
{
    /// <summary>
    ///     For exactly-once processing, it's necessary
    ///     to use a different producer instance per
    ///     partition being consumed from. Instances of
    ///     ProducerState are used to track these producer
    ///     instances and the offset of the last message
    ///     consumed from the corresponding input
    ///     partition.
    /// </summary>
    public class ProducerState<K,V>
    {
        public IProducer<K,V> Producer { get; set; }

        public Offset Offset { get; set; }
    }

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
        ///     The name of the input Kafka topic containing the lines of text to count the words in.
        ///         * key = null
        ///         * value = a line of text (type: string)
        /// </summary>
        const string Topic_InputLines = "lines";

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


        /// <summary>
        ///     The transactional id of the word splitting processor.
        /// </summary>
        const string TransactionalId_MapWords = "map-words-transaction-id";


        /// <summary>
        ///     The transactional id of the word count processor.
        /// </summary>
        const string TransactionalId_Aggregate = "aggregator-transaction-id";


        const string ConsumerGroup_MapWords = "map-words-consumer-group";

        const string ConsumerGroup_Aggregate = "aggregate-consumer-group";

        const string ConsumerGroup_LoadState = "load-state-consumer-group";


        /// <summary>
        ///     Remove all topics used by this example if they exist.
        /// </summary>
        static async Task DeleteTopics(string brokerList)
        {
            var config = new AdminClientConfig { BootstrapServers = brokerList };

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
        static async Task CreateTopicsMaybe(string brokerList)
        {
            var config = new AdminClientConfig { BootstrapServers = brokerList };

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
        static async Task Generator_LineInputData(string brokerList, CancellationToken ct)
        {
            var client = new HttpClient();
            var r = await client.GetAsync("https://raw.githubusercontent.com/confluentinc/confluent-kafka-dotnet/master/examples/Transactions/Program.cs", ct);
            r.EnsureSuccessStatusCode();
            var content = await r.Content.ReadAsStringAsync();
            var lines = content.Split('\n');

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
            };

            Console.WriteLine($"Producing text line data to topic: {Topic_InputLines}");
            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                var lCount = 0;
                foreach (var l in lines)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);  // slow down the calls to produce to make the output more interesting to watch.
                    await producer.ProduceAsync(Topic_InputLines, new Message<Null, string> { Value = l }, ct);
                    lCount += 1;
                    if (lCount % 10 == 0)
                    {
                        Console.WriteLine($"produced {lCount} input lines.");
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
            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_MapWords,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Offsets are committed using the producer as part of the
                // transaction - not the consumer. When using transactions,
                // you must turn off auto commit on the consumer, which is
                // enabled by default!
                EnableAutoCommit = false,
            };

            var TxnCommitPeriod = TimeSpan.FromSeconds(10);

            var lastTxnCommit = DateTime.Now;

            // Due to limitations outlined in KIP-447 (which KIP-447 overcomes), it is
            // currently necessary to use a separate producer per input partition. The
            // producerState dictionary is used to keep track of these, and the current
            // consumed offset.
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
            var producerState = new Dictionary<TopicPartition, ProducerState<string, Null>>();

            using (var consumer = new ConsumerBuilder<Null, string>(cConfig)
                .SetPartitionsRevokedHandler((c, partitions) => {
                    // Note: All handlers (except the log handler) are executed
                    // as a side-effect of, and on the same thread as the Consume
                    // or Close methods. Any exception thrown in a handler (with
                    // the exception of the log and error handlers) will
                    // be propagated to the application via the initiating
                    // call. i.e. in this example, any exceptions thrown in this
                    // handler will be exposed via the Consume method in the main
                    // consume loop and handler and handled by the try/catch block
                    // there.

                    // Abort any outstanding transactions & dispose producers
                    // corresponding to the previous generation of the consumer group.
                    var tasks = new List<Task>();
                    foreach (var p in producerState.Values)
                    {
                        tasks.Add(Task.Run(() => {
                            p.Producer.AbortTransaction(DefaultTimeout); // Note: Not cancellable yet.
                            p.Producer.Dispose();
                        }, ct));
                    }
                    if (tasks.Count > 0)
                    {
                        Console.WriteLine("Aborting current MapWords transactions.");
                        Task.WaitAll(tasks.ToArray());
                    }
                    producerState.Clear();
                })
                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** MapWords consumer group rebalanced. Partition assignment: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    Trace.Assert(producerState.Count == 0, "Unexpected producer state.");

                    // Then create a new set of producers for then new partition assignment
                    // and initialize.
                    var tasks = new List<Task>();
                    foreach (var tp in partitions)
                    {
                        tasks.Add(Task.Run(() => {
                            var pConfig = new ProducerConfig
                            {
                                BootstrapServers = brokerList,
                                TransactionalId = TransactionalId_MapWords + "-" + clientId + "-" + tp.Partition
                            };

                            var p = new ProducerBuilder<string, Null>(pConfig).Build();
                            p.InitTransactions(DefaultTimeout); // Note: Not cancellable yet.
                            p.BeginTransaction();
                            lock (producerState)
                            {
                                producerState.Add(tp, new ProducerState<string, Null> { Producer = p, Offset = Offset.Unset });
                            }
                        }, ct));
                    }
                    Task.WaitAll(tasks.ToArray());

                    // The PartitionsAssigned handler is called immediately after a
                    // new assignment set is received from the group coordinator and
                    // before that set is assigned to be read from. Since we have
                    // called init transactions already for the partitions relevant
                    // to this consumer, we can be sure that consumption will resume
                    // from the correct offsets (determined after this handler
                    // completed).
                })
                .Build())
            {
                consumer.Subscribe(Topic_InputLines); // Note: Subscribe is not blocking.

                var wCount = 0;
                var lCount = 0;
                while (true)
                {
                    try
                    {
                        ConsumeResult<Null, string> cr = consumer.Consume(ct);

                        lCount += 1;

                        producerState[cr.TopicPartition].Offset = cr.Offset;

                        var words = Regex.Split(cr.Message.Value.ToLower(), @"[^a-zA-Z_]").Where(s => s != String.Empty);
                        foreach (var w in words)
                        {
                            while (true)
                            {
                                try
                                {
                                    producerState[cr.TopicPartition].Producer.Produce(Topic_Words, new Message<string, Null> { Key = w });
                                    // Note: when using transactions, there is no need to check for errors of individual
                                    // produce call delivery reports because if the transaction commits successfully, you
                                    // can be sure that all the constituent messages were delivered successfully and in order.

                                    wCount += 1;
                                }
                                catch (KafkaException e)
                                {
                                    // An immediate failure of the produce call is most often caused by the
                                    // local message queue being full, and appropriate response to that is
                                    // to retry.
                                    if (e.Error.Code == ErrorCode.Local_QueueFull)
                                    {
                                        Thread.Sleep(TimeSpan.FromSeconds(1000));
                                        continue;
                                    }
                                    throw;
                                }
                                break;
                            }
                        }

                        // Commit transactions every TxnCommitPeriod
                        if (DateTime.Now > lastTxnCommit + TxnCommitPeriod)
                        {
                            // Execute the transaction commits for each producer in parallel.
                            var tasks = new List<Task>();
                            foreach (var state in producerState)
                            {
                                if (state.Value.Offset == Offset.Unset)
                                {
                                    continue;
                                }

                                tasks.Add(Task.Run(() =>
                                {
                                    state.Value.Producer.SendOffsetsToTransaction(
                                        // Note: committed offsets reflect the next message to consume, not last
                                        // message consumed, so we need to add one to the last consumed offset
                                        // values here.
                                        new List<TopicPartitionOffset> { new TopicPartitionOffset(state.Key, state.Value.Offset + 1) },
                                        consumer.ConsumerGroupMetadata, DefaultTimeout); // Note: Not cancellable yet.

                                    state.Value.Producer.CommitTransaction(DefaultTimeout);  // Note: Not cancellable yet.

                                    // Note: Exceptions thrown by SendOffsetsToTransaction and
                                    // CommitTransaction that are not marked as fatal can be
                                    // recovered from. However, in order to keep this example
                                    // short(er), the additional bookkeeping/logic required to
                                    // achieve this has been omitted. Since this should happen
                                    // only rarely, requiring a process restart in this case
                                    // isn't a huge compromise.

                                    state.Value.Offset = Offset.Unset;
                                    state.Value.Producer.BeginTransaction();
                                }));
                            }
                            Task.WaitAll(tasks.ToArray(), ct);

                            Console.WriteLine($"Committed MapWords transaction(s) comprising {wCount} words from {lCount} lines.");
                            lastTxnCommit = DateTime.Now;
                            wCount = 0;
                            lCount = 0;
                        }
                    }
                    catch (Exception)
                    {
                        // Note: transactions are aborted in the partitions revoked handler during close.
                        consumer.Close();
                        break;
                    }

                    // To simplify error handling, we assume the presence of a supervisor
                    // process that monitors whether worker processes have died, and restarts
                    // new instances as required. This is typical.
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
                        db.Put(Encoding.UTF8.GetBytes(cr.Message.Key), BitConverter.GetBytes(cr.Message.Value), columnFamily);
                    }
                }
            }

            Console.WriteLine($"Finished materializing word counts state. Backed by {msgCount} messages in Kafka topic '{Topic_Counts}'");
        }


        /// <summary>
        ///     A transactional (exactly once) processing loop that reads individual words and updates
        ///     the corresponding total count state.
        ///
        ///     When a rebalance occurs (including on startup), the total count state for all assigned
        ///     partitions is reloaded before the loop commences to update it.
        /// </summary>
        /// <remarks>
        ///     Refer to Processor_MapWords for more detailed comments.
        /// </remarks>
        public static void Processor_AggregateWords(string brokerList, string clientId, RocksDb db, CancellationToken ct)
        {
            var TxnCommitPeriod = TimeSpan.FromSeconds(10);

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = ConsumerGroup_Aggregate,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // This should be greater than the maximum amount of time required to read in
                // existing count state.
                MaxPollIntervalMs = 86400000,
                EnableAutoCommit = false
            };

            ColumnFamilyHandle columnFamily = null;

            var lastTxnCommit = DateTime.Now;

            var producerState = new Dictionary<TopicPartition, ProducerState<string, int>>();

            using (var consumer = new ConsumerBuilder<string, Null>(cConfig)
                .SetPartitionsRevokedHandler((c, partitions) => {
                    // clear rocksdb state.
                    db.DropColumnFamily("counts");
                    db.CreateColumnFamily(new ColumnFamilyOptions(), "counts");

                    var tasks = new List<Task>();
                    foreach (var p in producerState.Values)
                    {
                        tasks.Add(Task.Run(() => {
                            p.Producer.AbortTransaction(DefaultTimeout); // Note: Not cancellable yet.
                            p.Producer.Dispose();
                        }, ct));
                    }
                    if (tasks.Count > 0)
                    {
                        Console.WriteLine("Aborting current AggregateWords transactions.");
                    }
                    Task.WaitAll(tasks.ToArray());
                    producerState.Clear();
                })
                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** AggregateWords consumer group rebalanced. Partition assignment: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    Trace.Assert(producerState.Count == 0, "Unexpected producer state");

                    var tasks = new List<Task>();
                    foreach (var tp in partitions)
                    {
                        tasks.Add(Task.Run(() => {
                            var pConfig = new ProducerConfig
                            {
                                BootstrapServers = brokerList,
                                TransactionalId = TransactionalId_Aggregate + "-" + clientId + "-" + tp.Partition
                            };

                            var p = new ProducerBuilder<string, int>(pConfig).Build();
                            p.InitTransactions(DefaultTimeout); // Note: Not cancellable yet.
                            p.BeginTransaction();
                            lock (producerState)
                            {
                                producerState.Add(tp, new ProducerState<string, int> { Producer = p, Offset = Offset.Unset });
                            }
                        }, ct));
                    }
                    Task.WaitAll(tasks.ToArray());

                    columnFamily = db.GetColumnFamily("counts");
                    LoadCountState(db, brokerList, partitions.Select(p => p.Partition), columnFamily, ct);
                })
                .Build())
            {
                consumer.Subscribe(Topic_Words);

                var wCount = 0;

                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(ct);
                        producerState[cr.TopicPartition].Offset = cr.Offset;

                        var kBytes = Encoding.UTF8.GetBytes(cr.Message.Key);
                        var vBytes = db.Get(kBytes, columnFamily);
                        var v = vBytes == null ? 0 : BitConverter.ToInt32(vBytes);
                        var updatedV = v+1;

                        db.Put(kBytes, BitConverter.GetBytes(updatedV), columnFamily);

                        while (true)
                        {
                            try
                            {
                                producerState[cr.TopicPartition].Producer.Produce(
                                    Topic_Counts, new Message<string, int> { Key = cr.Message.Key, Value = updatedV });
                            }
                            catch (KafkaException e)
                            {
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
                            // Execute the transaction commits for each producer in parallel.
                            var tasks = new List<Task>();
                            foreach (var state in producerState)
                            {
                                if (state.Value.Offset == Offset.Unset)
                                {
                                    continue;
                                }

                                tasks.Add(Task.Run(() =>
                                {
                                    state.Value.Producer.SendOffsetsToTransaction(
                                        new List<TopicPartitionOffset> { new TopicPartitionOffset(state.Key, state.Value.Offset + 1) },
                                        consumer.ConsumerGroupMetadata, DefaultTimeout);
                                    state.Value.Producer.CommitTransaction(DefaultTimeout);
                                    state.Value.Offset = Offset.Unset;
                                    state.Value.Producer.BeginTransaction();
                                }));
                            }
                            Task.WaitAll(tasks.ToArray(), ct);

                            Console.WriteLine($"Committed AggregateWords transaction(s) comprising updates to {wCount} words.");
                            lastTxnCommit = DateTime.Now;
                            wCount = 0;
                        }
                    }
                    catch (Exception)
                    {
                        consumer.Close();
                        break;
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
            if (args.Length != 2 && args.Length != 3)
            {
                Console.WriteLine("Usage: .. <brokerList> gen|proc|del [client-id]");
                Console.WriteLine("   del:  delete all Kafka topics created by this example application.");
                Console.WriteLine("   gen:  generate line data (don't supply client-id)");
                Console.WriteLine("   proc: process line data. run multiple instances specifying different client-id's.");
                return;
            }

            string brokerList = args[0];
            string mode = args[1];

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            if (mode == "del")
            {
                await DeleteTopics(brokerList);
                return;
            }

            await CreateTopicsMaybe(brokerList);

            if (mode == "gen")
            {
                try { await Generator_LineInputData(brokerList, cts.Token); }
                catch (OperationCanceledException) {}
                return;
            }

            string clientId = args[2];

            RocksDb db = null;
            try
            {
                var options = new DbOptions()
                    .SetCreateIfMissing(true)
                    .SetCreateMissingColumnFamilies(true);

                var columnFamilies = new ColumnFamilies { { "counts", new ColumnFamilyOptions() } };

                db = RocksDb.Open(options, System.IO.Path.Join(System.IO.Path.GetTempPath(), "rocksdb_" + Guid.NewGuid().ToString()), columnFamilies);

                var processors = new List<Task>();
                processors.Add(Task.Run(() => Processor_MapWords(brokerList, clientId, cts.Token)));
                processors.Add(Task.Run(() => Processor_AggregateWords(brokerList, clientId, db, cts.Token)));
                processors.Add(Task.Run(async () => await PeriodicallyDisplayTopCountsState(brokerList, db, cts.Token)));

                var allTasks = await Task.WhenAny(processors.ToArray());
                if (allTasks.IsFaulted)
                {
                    if (!(allTasks.Exception.InnerException is OperationCanceledException))
                    {
                        Console.WriteLine(allTasks.Exception.InnerException.ToString());
                    }
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
