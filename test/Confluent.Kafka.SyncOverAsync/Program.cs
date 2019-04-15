using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


// This program is included as an educational tool to allow you to
// experiment with different scenarios that may cause deadlocks.
//
// For more information, two good reasourcs are:
//   https://devblogs.microsoft.com/pfxteam/should-i-expose-synchronous-wrappers-for-asynchronous-methods/
//   https://blog.stephencleary.com/2012/07/dont-block-on-async-code.html
//   https://blogs.msdn.microsoft.com/vancem/2018/10/16/diagnosing-net-core-threadpool-starvation-with-perfview-why-my-service-is-not-saturating-all-cores-or-seems-to-stall/

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
                BootstrapServers = args[0]
            };
            
            using (var producer = new ProducerBuilder<Null, string>(pConfig)
                .SetValueSerializer(new SimpleAsyncSerializer().SyncOverAsync()) // may deadlock due to thread pool exhaustion.
                // .SetValueSerializer(new SimpleSyncSerializer()) // will never deadlock.
                .Build())
            {
                var topic = Guid.NewGuid().ToString();
                var tasks = new List<Task>();

                // will deadlock if N >= workerThreads.
                int N = workerThreads;
                for (int i=0; i<N; ++i)
                {
                    // create a unique delvery report handler for each task.
                    Func<int, Action> actionCreator = (taskNumber) =>
                    {
                        return () =>
                        {
                            Console.WriteLine($"running task {taskNumber}");
                            object waitObj = new object();

                            Action<DeliveryReport<Null, string>> handler = dr => 
                            {
                                // in a deadlock scenario, the delivery handler will
                                // never execute since execution of the BeginProduce
                                // method calls never progresses past serialization.
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

                            // in a deadlock scenario, this line never be hit, since the
                            // serializer blocks during the BeginProduce call.
                            Console.WriteLine($"waiting for delivery report {taskNumber}");

                            lock (waitObj)
                            {
                                Monitor.Wait(waitObj);
                            }
                        };
                    };

                    tasks.Add(Task.Run(actionCreator(i)));
                }

                Console.WriteLine($"waiting for {tasks.Count} produce tasks to complete. --> expecting deadlock <--");
                Task.WaitAll(tasks.ToArray());

                Console.WriteLine($"number outstanding produce requests on exit: {producer.Flush(TimeSpan.FromSeconds(10))}");
            }
        }
    }
}
