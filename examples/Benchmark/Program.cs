using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        public class DeliveryHandler : IDeliveryHandler
        {
            public void SetException(Exception exception)
            {
                throw exception;
            }

            public void SetResult(DeliveryReport deliveryReport)
            {
            }
        }

        public static void Produce(string broker, string topicName, long numMessages)
        {
            var deliveryHandler = new DeliveryHandler();

            var config = new Dictionary<string, string> { { "bootstrap.servers", broker } };

            using (var producer = new Producer<Null, byte[]>(config, null))
            {
                // TODO: remove need to explicitly specify this serializer.
                producer.ValueSerializer = (ISerializer<byte[]>)new ByteArraySerializer();

                Console.WriteLine($"{producer.Name} producing on {topicName}");
                // TODO: think more about exactly what we want to benchmark.
                var payload = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
                for (int i = 0; i < numMessages; i++)
                {
                    producer.ProduceWithDeliveryReport(topicName, payload, deliveryHandler);
                }

                Console.WriteLine("Shutting down");
            }
        }

        public static async Task<long> Consume(string broker, string topic)
        {
            long n = 0;

            var defaultTopicConfig = new Dictionary<string, string>
            {
                { "auto.offset.reset", "smallest" }
            };

            var config = new Dictionary<string, string>
            {
                { "bootstrap.servers", broker },
                { "group.id", "benchmark-consumer" }
            };

            // TODO(mhowlett): merge defaultTopicConfig and config.
            using (var consumer = new EventConsumer(config, defaultTopicConfig))
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

            long numMessages = 1000000;

            var stopwatch = new Stopwatch();

            // TODO: we really want time from first ack. as it is, this includes producer startup time.
            stopwatch.Start();
            Produce(brokerList, topic, numMessages);
            stopwatch.Stop();

            Console.WriteLine($"Sent {numMessages} messages in {stopwatch.Elapsed}");
            Console.WriteLine($"{numMessages / stopwatch.Elapsed.TotalSeconds:F0} messages/second");

            stopwatch.Restart();
            long n = Consume(brokerList, topic).Result;
            stopwatch.Stop();

            Console.WriteLine($"Received {n} messages in {stopwatch.Elapsed}");
            Console.WriteLine($"{n / stopwatch.Elapsed.TotalSeconds:F0} messages/second");
        }
    }
}
