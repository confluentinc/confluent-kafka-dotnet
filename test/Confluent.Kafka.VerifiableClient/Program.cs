using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using System.Threading;

namespace Confluent.Kafka.VerifiableClient
{

    public class VerifiableClient
    {
        private bool Running; // Continue to run, set to false to terminate
        private ManualResetEvent TerminateEvent; // Termination wake-up event

        public VerifiableClient()
        {
            Running = true;
            TerminateEvent = new ManualResetEvent(false);
        }

        public void Send(string name, Dictionary<string, string> dict)
        {
            dict["name"] = name;
            dict["_time"] = DateTime.UtcNow.ToString();
            dict["_thread"] = Thread.CurrentThread.ManagedThreadId.ToString();
            Console.WriteLine(JsonConvert.SerializeObject(dict));
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
            Dbg("Stopping: " + reason);
            Running = false;
            TerminateEvent.Set();
        }

        public void WaitTerm ()
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

        public VerifiableClientConfig()
        {
            this.Conf = new Dictionary<string, object>
                { { "default.topic.config", new Dictionary<string, object>() } };
        }
    }

    public class VerifiableProducerConfig : VerifiableClientConfig
    {
        public double MsgRate = 100.0;  // Messages/second
        public int MaxMsgs = 100000;
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

        public void Dispose()
        {
            Dbg("Disposing of producer");
            Handle.Dispose();
        }


        public void HandleDelivery(Message<Null, string> msg)
        {
            var d = new Dictionary<string, string>
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

        public void Run ()
        {
            Send("startup_complete", new Dictionary<string, string>());

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

            Send("shutdown_complete", new Dictionary<string, string>());
        }

    }

    public class Program
    {
        static private void Usage (int exitCode, string reason)
        {
            if (reason.Length > 0)
                Console.Error.WriteLine($"Error: {reason}");

            Console.Error.WriteLine(@".NET VerifiableProducer for kafkatest
Usage: .. --option1 val1 --option2 val2 ..

Options:
   --topic <topic>              Topic to produce to (required)
   --broker-list <brokers,..>   Bootstrap brokers (required)
   --throughput <msgs/s>        Message rate
   --max-messages <msgs>        Max messages to produce
   --value-prefix <string>      Message value prefix string
   --acks <n|all>               Required acks
   --producer.config <file>     Ignored (compatibility)
   --debug <debugfac,..>        librdkafka debugging facilities
   --property <k=v,..>          librdkafka configuration properties
");
            Environment.Exit(exitCode);
        }


        static private VerifiableProducerConfig ArgParse(string[] args)
        {
            var clientConf = new VerifiableProducerConfig();
            for (var i = 0; i + 1 < args.Length; i += 2)
            {
                var key = args[i];
                var val = args[i + 1];
                Console.Error.WriteLine($"Arg: {key} {val}");
                switch (key)
                {
                    case "--topic":
                        clientConf.Topic = val;
                        break;
                    case "--broker-list":
                        clientConf.Conf["bootstrap.servers"] = val;
                        break;
                    case "--throughput":
                        clientConf.MsgRate = double.Parse(val);
                        break;
                    case "--max-messages":
                        clientConf.MaxMsgs = int.Parse(val);
                        break;
                    case "--value-prefix":
                        clientConf.ValuePrefix = val;
                        break;
                    case "--acks":
                        ((Dictionary<string,object>)clientConf.Conf["default.topic.config"])["acks"] = val;
                        break;
                    case "--producer.config":
                        /* Ignored */
                        break;
                    case "--debug":
                        clientConf.Conf["debug"] = val;
                        break;
                    case "--property":
                        foreach (var kv in val.Split(','))
                        {
                            var kva = kv.Split('=');
                            if (kva.Length != 2)
                                Usage(1, $"Invalid property: {kv}");

                            clientConf.Conf[kva[0]] = kva[1];
                        }
                        break;
                    default:
                        Usage(1, $"Invalid option: {key} {val}");
                        break;
                }
            }

            if (clientConf.Topic.Length == 0)
                Usage(1, "Missing --topic ..");

            return clientConf;
        }
        public static void Main(string[] args)
        {
            var clientConf = ArgParse(args);

            using (var producer = new VerifiableProducer(clientConf))
            {
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    producer.Stop("Terminated by user");
                };

                producer.Run();
            }
            return;
        }
    }
}
