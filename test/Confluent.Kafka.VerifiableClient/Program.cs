// Copyright 2017 Confluent Inc.
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
using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Threading;

namespace Confluent.Kafka.VerifiableClient
{
    // JSON serializer that generates lower-case keys.
    public class LowercaseJsonSerializer
    {
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ContractResolver = new LowercaseContractResolver()
        };

        public static string SerializeObject(object o)
            => JsonConvert.SerializeObject(o, Formatting.None, Settings);

        public class LowercaseContractResolver : DefaultContractResolver
        {
            protected override string ResolvePropertyName(string propertyName)
                => propertyName.ToLower();
        }
    }

    public class VerifiableClient : IDisposable
    {
        public bool Running; // Continue to run, set to false to terminate
        public ManualResetEvent TerminateEvent; // Termination wake-up event

        public VerifiableClient()
        {
            Running = true;
            TerminateEvent = new ManualResetEvent(false);
        }

        public void Send(string name, Dictionary<string, object> dict)
        {
            dict["name"] = name;
            dict["_time"] = DateTime.UtcNow.ToString();
            dict["_thread"] = Thread.CurrentThread.ManagedThreadId.ToString();
            Console.WriteLine(LowercaseJsonSerializer.SerializeObject(dict));
        }

        public void Dbg(string text)
        {
            var time = DateTime.UtcNow.ToString();
            Console.Error.WriteLine($"DEBUG: {time}: {text}");
        }

        public void Fatal(string text)
        {
            var time = DateTime.UtcNow.ToString();
            Console.Error.WriteLine($"FATAL: {time}: {text}");
            Environment.Exit(1);
        }

        public void Stop(string reason)
        {
            if (!Running)
                return;
            Dbg("Stopping: " + reason);
            Running = false;
            TerminateEvent.Set();
        }

        public virtual void Run()
        {
            throw new NotImplementedException();
        }

        public virtual void Dispose()
        {
            throw new NotImplementedException();
        }

        public virtual void WaitTerm ()
        {
            do
            {
                TerminateEvent.WaitOne();
            } while (Running);
        }
    }

    public class VerifiableClientConfig
    {
        public string Topic;
        public Dictionary<string, object> Conf;
        public int MaxMsgs = 100000;

        public VerifiableClientConfig()
        {
            this.Conf = new Dictionary<string, object>
                { { "log.thread.name", true },
                  { "api.version.request", true },
                  { "default.topic.config", new Dictionary<string, object>() } };
        }
    }

    public class VerifiableProducerConfig : VerifiableClientConfig
    {
        public double MsgRate = 100.0;  // Messages/second
        public string ValuePrefix;

        public VerifiableProducerConfig ()
        {
            // Default Producer configs
            ((Dictionary<string, object>)Conf["default.topic.config"])["acks"] = "all";
        }
    }

    public class VerifiableProducer : VerifiableClient, IDisposable
    {
        Producer<Null, string> Handle; // Client Handle

        long DeliveryCnt; // Successfully delivered messages
        long ErrCnt; // Failed deliveries
        long MsgCnt; // Number of Produce()d messages
        DateTime LastProduce; // Time of last produce()
        private object ProduceLock;  // Protects MsgCnt,LastProduce while Producing so that Produces() are sequencial
        System.Threading.Timer ProduceTimer; // Producer rate-limiter timer
        VerifiableProducerConfig Config;
        DeliveryHandler deliveryHandler;

        public VerifiableProducer(VerifiableProducerConfig clientConfig)
        {
            Config = clientConfig;
            Handle = new Producer<Null, string>(Config.Conf, new NullSerializer(), new StringSerializer(Encoding.UTF8));
            ProduceLock = new object();
            deliveryHandler = new DeliveryHandler(this);
            Dbg("Created producer " + Handle.Name);
        }

        ~VerifiableProducer ()
        {
            Dbg("Destruction of producer");
        }

        public override void Dispose()
        {
            Dbg("Disposing of producer");
            if (Handle != null)
            {
                Handle.Dispose();
            }
        }


        public void HandleDelivery(Message<Null, string> msg)
        {
            var d = new Dictionary<string, object>
                    {
                        { "topic", msg.Topic },
                        { "partition", msg.TopicPartition.Partition.ToString() },
                        { "key", $"{msg.Key}" }, // always Null
                        { "value", $"{msg.Value}" }
                    };

            if (msg.Error)
            {
                this.ErrCnt++;
                d["message"] = msg.Error.ToString();
                this.Send("producer_send_error", d);
            }
            else
            {
                this.DeliveryCnt++;
                d["offset"] = msg.Offset.ToString();
                d["_DeliveryCnt"] = DeliveryCnt.ToString();
                lock (ProduceLock)
                    d["_ProduceCnt"] = MsgCnt.ToString();
                this.Send("producer_send_success", d);
            }

            if (ErrCnt + DeliveryCnt >= Config.MaxMsgs)
                Stop($"All messages accounted for: {DeliveryCnt} delivered + {ErrCnt} failed >= {Config.MaxMsgs}");
        }

        private class DeliveryHandler : IDeliveryHandler<Null, string>
        {
            private VerifiableProducer vp;
            public DeliveryHandler (VerifiableProducer producer)
            {
                vp = producer;
            }

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(Message<Null, string> msg)
            {
                vp.HandleDelivery(msg);
            }
        }


        private void Produce (string topic, string value)
        {
            try
            {
                Handle.ProduceAsync(topic, null, value, deliveryHandler);
            } catch (KafkaException e)
            {
                Fatal($"Produce({topic}, {value}) failed: {e}");
            }
        }

        private void TimedProduce(object ignore)
        {
            lock (ProduceLock)
            {
                double elapsed = (DateTime.Now - LastProduce).TotalMilliseconds/1000.0;
                double msgsToSend = Config.MsgRate * elapsed;
                if (msgsToSend < 1)
                    return;
                for (var i = 0; i < (int)msgsToSend && MsgCnt < Config.MaxMsgs; i++, MsgCnt++)
                    Produce(Config.Topic, $"{Config.ValuePrefix}{MsgCnt}");
                if (MsgCnt >= Config.MaxMsgs)
                {
                    Dbg($"All {MsgCnt} messages produced, waiting for {MsgCnt - DeliveryCnt} deliveries...");
                    ProduceTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    return;
                }
                LastProduce = DateTime.Now;
            }
        }

        public override void Run ()
        {
            Send("startup_complete", new Dictionary<string, object>());

            // Calculate an appropriate wake-up time to fullfil throughput
            // requirements, with at most 1000 wake-ups per second.
            var wakeupInterval = (int)Math.Max(1000 / Config.MsgRate, 1);

            Dbg($"Set wakeupInterval to {wakeupInterval} based on MsgRate {Config.MsgRate}");
            LastProduce = DateTime.Now;
            ProduceTimer = new System.Threading.Timer(TimedProduce, null, 0, wakeupInterval);

            // Wait for termination
            WaitTerm();

            ProduceTimer.Dispose();

            Dbg("Flushing Producer");
            var remain = Handle.Flush(10000);
            Dbg($"{remain} messages remain in queue after flush");

            // Explicitly handle dispose to catch hang-on-dispose errors
            Handle.Dispose();
            Handle = null;

            Send("shutdown_complete", new Dictionary<string, object>());
        }

    }

    public class VerifiableConsumerConfig : VerifiableClientConfig
    {
        public bool AutoCommit;

        public VerifiableConsumerConfig()
        {
            // Default Consumer configs
            Conf["session.timeout.ms"] = 6000;
            ((Dictionary<string, object>)Conf["default.topic.config"])["auto.offset.reset"] = "smallest";
        }
    }


    public class VerifiableConsumer : VerifiableClient, IDisposable
    {
        Consumer<Null, string> Handle; // Client Handle
        VerifiableConsumerConfig Config;

        private Dictionary<TopicPartition,AssignedPartition> currentAssignment;
        private int consumedMsgs;
        private int consumedMsgsLastReported;
        private int consumedMsgsAtLastCommit;
        private const int commitEvery = 1000; // commit interval (messages)

        private class AssignedPartition
        {
            public int ConsumedMsgs;
            public Int64 MinOffset;
            public Int64 MaxOffset;

            public AssignedPartition()
            {
                MinOffset = -1;
                MaxOffset = -1;
            }
        };


        public VerifiableConsumer(VerifiableConsumerConfig clientConfig)
        {
            Config = clientConfig;
            Config.Conf["enable.auto.commit"] = Config.AutoCommit;
            Handle = new Consumer<Null, string>(Config.Conf, new NullDeserializer(), new StringDeserializer(Encoding.UTF8));
            consumedMsgsAtLastCommit = 0;
            Dbg($"Created Consumer {Handle.Name} with AutoCommit={Config.AutoCommit}");
        }

        /// <summary>
        ///   Override WaitTerm to periodically send records_consumed stats to driver
        /// </summary>
        public override void WaitTerm ()
        {
            do
            {
                SendRecordsConsumed(true);
                TerminateEvent.WaitOne(1000);
            } while (Running);
        }

        public override void Dispose()
        {
            Dbg($"Disposing of Consumer {Handle}");
            if (Handle != null)
            {
                Handle.Dispose();
            }
        }

        /// <summary>
        /// Send "records_consumed" to test driver
        /// </summary>
        /// <param name="immediate">Force send regardless of interval</param>
        private void SendRecordsConsumed (bool immediate)
        {
            // Report every 1000 messages, or immediately if forced
            if (currentAssignment == null ||
                (!immediate &&
                consumedMsgsLastReported + 1000 > consumedMsgs))
                return;

            // assigned partitions list
            var alist = new List<Dictionary<string, object>>();
            foreach (var item in currentAssignment)
            {
                var ap = item.Value;
                if (ap.MinOffset == -1)
                    continue; // Skip partitions with no new messages since last run
                alist.Add(new Dictionary<string, object>
                {
                    { "topic", item.Key.Topic },
                    { "partition", item.Key.Partition },
                    { "consumed_msgs", ap.ConsumedMsgs },
                    { "minOffset", ap.MinOffset },
                    { "maxOffset", ap.MaxOffset }
                });
                ap.MinOffset = -1;
            }

            if (alist.Count == 0)
                return;

            var d = new Dictionary<string, object>
            {
                { "count", consumedMsgs - consumedMsgsLastReported  },
                { "partitions", alist }
            };

            Send("records_consumed", d);

            consumedMsgsLastReported = consumedMsgs;
        }

        /// <summary>
        /// Send result of offset commit to test-driver
        /// </summary>
        /// <param name="offsets">Committed offsets</param>
        private void SendOffsetsCommitted (CommittedOffsets offsets)
        {
            Dbg($"OffsetCommit {offsets.Error}: {string.Join(", ", offsets.Offsets)}");
            if (offsets.Error.Code == ErrorCode._NO_OFFSET)
                return; // no offsets to commit, ignore
            var d = new Dictionary<string, object>{
                    { "success", (bool)!offsets.Error },
                    { "offsets", offsets.Offsets },
                };
            if (offsets.Error)
                d["error"] = offsets.Error.ToString();
            Send("offsets_committed", d);
        }

        /// <summary>
        /// Commit offsets every commitEvery messages or when immediate is true.
        /// </summary>
        /// <param name="immediate"></param>
        private void Commit (bool immediate)
        {
            if (!immediate &&
                (Config.AutoCommit ||
                consumedMsgsAtLastCommit + commitEvery > consumedMsgs))
                return;

            Dbg($"Committing {consumedMsgs - consumedMsgsAtLastCommit} messages");

            consumedMsgsAtLastCommit = consumedMsgs;

            SendOffsetsCommitted(Handle.CommitAsync().Result);
        }


        /// <summary>
        /// Handle consumed message (or consumer error)
        /// </summary>
        /// <param name="m"></param>
        private void HandleMessage (Message<Null,string> m)
        {
            AssignedPartition ap;

            if (currentAssignment == null ||
                !currentAssignment.TryGetValue(m.TopicPartition, out ap)) {
                Dbg($"Received Message on unassigned partition {m.TopicPartition}");
                return;
            }

            if (m.Error.Code != ErrorCode.NO_ERROR)
            {
                Dbg($"Message error {m.Error} at {m.TopicPartitionOffset}");
                return;
            }

            consumedMsgs++;
            ap.ConsumedMsgs++;

            if (ap.MinOffset == -1)
                ap.MinOffset = m.Offset;

            if (ap.MaxOffset < m.Offset)
                ap.MaxOffset = m.Offset;


            SendRecordsConsumed(false);

            Commit(false);

            if (Config.MaxMsgs > 0 && consumedMsgs > Config.MaxMsgs)
                Stop($"Consumed all {consumedMsgs}/{Config.MaxMsgs} messages");
        }


        /// <summary>
        /// Send partition list to test-driver
        /// </summary>
        /// <param name="name"></param>
        /// <param name="partitions"></param>
        private void SendPartitions (string name, ICollection<TopicPartition> partitions)
        {
            var list = new List<Dictionary<string, object>>();

            foreach (var p in partitions)
            {
                list.Add(new Dictionary<string, object>
                {
                    { "topic", p.Topic },
                    { "partition", p.Partition }
                });
            }

            Send(name, new Dictionary<string, object> { { "partitions", list } });
        }


        /// <summary>
        /// Handle new partition assignment
        /// </summary>
        /// <param name="partitions"></param>
        private void HandleAssign (ICollection<TopicPartition> partitions)
        {
            Dbg($"New assignment: {string.Join(", ", partitions)}");
            if (currentAssignment != null)
                Fatal($"Received new assignment {partitions} with already existing assignment in place: {currentAssignment}");

            currentAssignment = new Dictionary<TopicPartition, AssignedPartition>();

            foreach (var p in partitions)
                currentAssignment[p] = new AssignedPartition();

            Handle.Assign(partitions);

            SendPartitions("partitions_assigned", partitions);
        }

        /// <summary>
        /// Handle partition revocal
        /// </summary>
        private void HandleRevoke (ICollection<TopicPartition> partitions)
        {
            Dbg($"Revoked assignment: {string.Join(", ", partitions)}");
            if (currentAssignment == null)
                Fatal($"Received revoke of {partitions} with no current assignment");

            SendRecordsConsumed(true/*immediate*/);
            Commit(true/*immediate*/);

            currentAssignment = null;

            Handle.Unassign();

            SendPartitions("partitions_revoked", partitions);
        }


        public override void Run()
        {
            Send("startup_complete", new Dictionary<string, object>());

            Handle.OnMessage += (_, msg)
                => HandleMessage(msg);

            Handle.OnError += (_, error)
                => Fatal($"Error: {error}");

            Handle.OnPartitionsAssigned += (_, partitions)
                => HandleAssign(partitions);

            Handle.OnPartitionsRevoked += (_, partitions)
                => HandleRevoke(partitions);

            // Only used when auto-commits enabled
            Handle.OnOffsetsCommitted += (_, offsets)
                => SendOffsetsCommitted(offsets);

            Handle.Start();
            Handle.Subscribe(Config.Topic);

            // Wait for termination
            WaitTerm();

            // Explicitly handle dispose to catch hang-on-dispose errors
            Dbg("Closing Consumer");
            Handle.Dispose();
            Handle = null;

            Send("shutdown_complete", new Dictionary<string, object>());
        }
    }



    public class Program
    {
        static private void Usage (int exitCode, string reason)
        {
            if (reason.Length > 0)
                Console.Error.WriteLine($"Error: {reason}");

            Console.Error.WriteLine(@".NET VerifiableClient for kafkatest
Usage: .. --consumer|--producer --option1 val1 --option2 val2 ..

Mode:
   --consumer                   Run VerifiableConsumer
   --producer                   Run VerifiableProducer

Options:
   --topic <topic>              Topic to produce/consume (required)
   --broker-list <brokers,..>   Bootstrap brokers (required)
   --max-messages <msgs>        Max messages to produce/consume
   --debug <debugfac,..>        librdkafka debugging facilities
   --property <k=v,..>          librdkafka configuration properties

Producer options:
   --throughput <msgs/s>        Message rate
   --value-prefix <string>      Message value prefix string
   --acks <n|all>               Required acks
   --producer.config <file>     Ignored (compatibility)

Consumer options:
   --group <group>              Consumer group (required)
   --session-timeout <ms>       Group session timeout
   --enable-autocommit <bool>   Enable/disable auto commit (false)
   --assignment-strategy <jcls> Java assignment strategy class name
   --consumer.config <file>     Ignored (compatibility)

");
            Environment.Exit(exitCode);
        }

        /**
         *  @brief Translates a CSV-list of Java assignment strategy class names
         *         to their librdkafka counterparts (lower-case without fluff).
         */
        static private string JavaAssignmentStrategyParse (string javas)
        {
            var re = new Regex(@"org.apache.kafka.clients.consumer.(\w+)Assignor");
            return re.Replace(javas, "$1").ToLower();
        }

        static private void AssertProducer (string mode, string key)
        {
            if (!mode.Equals("--producer"))
                Usage(1, $"{key} is a producer property");
        }

        static private void AssertConsumer(string mode, string key)
        {
            if (!mode.Equals("--consumer"))
                Usage(1, $"{key} is a consumer property");
        }

        static private VerifiableClient NewClientFromArgs(string[] args)
        {
            VerifiableClientConfig conf = null; // avoid warning
            string mode = "";

            if (args.Length < 1)
                Usage(1, "--consumer or --producer must be specified");
            mode = args[0];
            if (mode.Equals("--producer"))
                conf = new VerifiableProducerConfig();
            else if (mode.Equals("--consumer"))
                conf = new VerifiableConsumerConfig();
            else
                Usage(1, "--consumer or --producer must be the first argument");

            for (var i = 1; i + 1 < args.Length; i += 2)
            {
                var key = args[i];
                var val = args[i + 1];

                // It is helpful to see the passed arguments from system test logs
                Console.Error.WriteLine($"{mode} Arg: {key} {val}");
                switch (key)
                {
                    /* Generic options */
                    case "--topic":
                        conf.Topic = val;
                        break;
                    case "--broker-list":
                        conf.Conf["bootstrap.servers"] = val;
                        break;
                    case "--max-messages":
                        conf.MaxMsgs = int.Parse(val);
                        break;
                    case "--debug":
                        conf.Conf["debug"] = val;
                        break;
                    case "--property":
                        foreach (var kv in val.Split(','))
                        {
                            var kva = kv.Split('=');
                            if (kva.Length != 2)
                                Usage(1, $"Invalid property: {kv}");

                            conf.Conf[kva[0]] = kva[1];
                        }
                        break;

                    /* Producer options */
                    case "--throughput":
                        AssertProducer(mode, key);
                        ((VerifiableProducerConfig)conf).MsgRate = double.Parse(val);
                        break;
                    case "--value-prefix":
                        AssertProducer(mode, key);
                        ((VerifiableProducerConfig)conf).ValuePrefix = val;
                        break;
                    case "--acks":
                        AssertProducer(mode, key);
                        ((Dictionary<string,object>)conf.Conf["default.topic.config"])["acks"] = val;
                        break;
                    case "--producer.config":
                        /* Ignored */
                        break;

                    /* Consumer options */
                    case "--group-id":
                        AssertConsumer(mode, key);
                        conf.Conf["group.id"] = val;
                        break;
                    case "--session-timeout":
                        AssertConsumer(mode, key);
                        conf.Conf["session.timeout.ms"] = int.Parse(val);
                        break;
                    case "--enable-autocommit":
                        AssertConsumer(mode, key);
                        ((VerifiableConsumerConfig)conf).AutoCommit = bool.Parse(val);
                        break;
                    case "--assignment-strategy":
                        AssertConsumer(mode, key);
                        conf.Conf["partition.assignment.strategy"] = JavaAssignmentStrategyParse(val);
                        break;

                    default:
                        Usage(1, $"Invalid option: {key} {val}");
                        break;
                }
            }

            if (conf.Topic.Length == 0)
                Usage(1, "Missing --topic ..");

            Console.Error.WriteLine($"Running {mode} using librdkafka {Confluent.Kafka.Library.VersionString} ({Confluent.Kafka.Library.Version:x})");
            if (mode.Equals("--producer"))
                return new VerifiableProducer(((VerifiableProducerConfig)conf));
            else
                return new VerifiableConsumer(((VerifiableConsumerConfig)conf));
        }

        public static void Main(string[] args)
        {
            using (var client = NewClientFromArgs(args))
            {
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    if (!client.Running)
                    {
                        client.Stop("Forced termination");
                        Environment.Exit(1);
                    }
                    else
                    {
                        client.Stop("Terminated by user");
                    }
                };

                client.Run();
            }

            return;
        }
    }
}

// --producer --broker-list eden --topic test.net --acks all --property log.thread.name=true --debug broker --max-messages 1000000 --throughput 10
// --consumer --broker-list eden --topic test.net --group-id g1 --property log.thread.name=true --debug broker,cgrp,topic,fetch,all --max-messages 100
