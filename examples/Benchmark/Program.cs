using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        public class BenchmarkProducer
        {
            public static void Run(string broker, string topicName, int numberOfMessagesToProduce, int numberOfTests)
            {
                // mirrors the librdkafka performance test example.
                var config = new Dictionary<string, object>
                {
                    { "bootstrap.servers", broker },
                    { "queue.buffering.max.messages", 500000 },
                    { "message.send.max.retries", 3 },
                    { "retry.backoff.ms", 500 },
                    { "session.timeout.ms", 6000 }
                };

                using (var producer = new Producer(config))
                {
                    for (var j=0; j<numberOfTests; ++j)
                    {
                        Console.WriteLine($"{producer.Name} producing on {topicName}");

                        byte cnt = 0;
                        var val = new byte[100].Select(a => ++cnt).ToArray();

                        var startTime = DateTime.Now.Ticks;
                        var tasks = new Task[numberOfMessagesToProduce];
                        for (int i = 0; i < numberOfMessagesToProduce; i++)
                        {
                            tasks[i] = producer.ProduceAsync(topicName, null, val);
                        }
                        Task.WaitAll(tasks);
                        var duration = DateTime.Now.Ticks - startTime;

                        Console.WriteLine($"Produced {numberOfMessagesToProduce} in {duration/10000.0:F0}ms");
                        Console.WriteLine($"{numberOfMessagesToProduce / (duration/10000.0):F0} messages/ms");
                    }

                    Console.WriteLine("Disposing producer");
                }
            }
        }

        // TODO: Update Consumer benchmark for new Consumer when it's written.
        public static async Task<long> Consume(string broker, string topic)
        {
            long n = 0;

            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", broker },
                { "group.id", "benchmark-consumer" },
                { "default.topic.config", new Dictionary<string, object> { { "auto.offset.reset", "smallest" } } }
            };

            using (var consumer = new EventConsumer(config))
            {
                var signal = new SemaphoreSlim(0, 1);

                consumer.OnMessage += (obj, msg) =>
                {
                    n += 1;
                };

                consumer.OnEndReached += (obj, end) =>
                {
                    Console.WriteLine($"End reached");
                    signal.Release();
                };

                consumer.Subscribe(new List<string>{topic});
                consumer.Start();

                await signal.WaitAsync();
                Console.WriteLine($"Shutting down");
            }

            return n;
        }

        public static void Main(string[] args)
        {
            string brokerList = args[0];
            string topic = args[1];

            BenchmarkProducer.Run(brokerList, topic, 5000000, 4);
        }
    }
}
