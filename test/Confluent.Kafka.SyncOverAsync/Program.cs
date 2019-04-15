using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;


namespace Confluent.Kafka.SyncOverAsync
{
    class SimpleAsyncSerializer : IAsyncSerializer<string>
    {
        public async Task<byte[]> SerializeAsync(string data, SerializationContext context)
        {
            await Task.Delay(500);
            return Serializers.Utf8.Serialize(data, context);
        }

        public ISerializer<string> SyncOverAsync()
        {
            return new SyncOverAsyncSerializer<string>(this);
        }
    }

    class SimpleSyncSerializer : ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext context)
        {
            // real sync.
            Thread.Sleep(500);
            return Serializers.Utf8.Serialize(data, context);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            ThreadPool.GetMinThreads(out int workerThreads, out int completionPortThreads);   
            ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine($"ThreadPool workerThreads: {workerThreads},  completionPortThreads: {completionPortThreads}");

            var pConfig = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                Acks = Acks.All,
                Debug = "all"
            };

            var srConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "127.0.0.1:8081"
            };
            
            using (var producer = new ProducerBuilder<Null, string>(pConfig)
                .SetValueSerializer(new SimpleAsyncSerializer().SyncOverAsync())
                //.SetValueSerializer(new SimpleSyncSerializer())
                .Build())
            {
                var topic = Guid.NewGuid().ToString();
                var tasks = new List<Task>();

                // workerThreads-1 tasks will not deadlock. workerThreads will.
                for (int i=0; i<workerThreads; ++i)
                {
                    // creates a unique delvery report handler for each task.
                    Func<int, Action> actionCreator = (taskNumber) =>
                    {
                        return () =>
                        {
                            Console.WriteLine($"running task {taskNumber}");
                            object waitObj = new object();

                            Action<DeliveryReport<Null, string>> handler = dr => 
                            {
                                Console.WriteLine($"delivery report: {dr.Value}");
                                lock (waitObj)
                                {
                                    Monitor.Pulse(waitObj);
                                }
                            };

                            try
                            {
                                producer.BeginProduce(topic, new Message<Null, string> { Value = $"value: {taskNumber}" }, handler);
                                // will never get to after BeginProduce, because deadlock occurs when running serializers.
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.StackTrace);
                            }

                            // will never hit.
                            Console.WriteLine($"waiting for delivery report {taskNumber}");

                            lock (waitObj)
                            {
                                Monitor.Wait(waitObj);
                            }
                        };
                    };

                    tasks.Add(Task.Run(actionCreator(i)));
                }

                Console.WriteLine($"waiting for {tasks.Count} produce tasks to complete");
                Task.WaitAll(tasks.ToArray());

                Console.WriteLine($"number outstanding produce requests on exit: {producer.Flush(TimeSpan.FromSeconds(10))}");
            }
        }
    }
}
