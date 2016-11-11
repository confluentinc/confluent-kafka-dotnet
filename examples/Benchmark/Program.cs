using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

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

            using (var producer = new Producer(broker))
            using (Topic topic = producer.Topic(topicName))
            {
                Console.WriteLine($"{producer.Name} producing on {topic.Name}");
                for (int i = 0; i < numMessages; i++)
                {
                    byte[] data = Encoding.UTF8.GetBytes(i.ToString());
                    topic.Produce(data, deliveryHandler);
                }

                Console.WriteLine("Shutting down");
            }
        }

        public static async Task<long> Consume(string broker, string topic)
        {
            long n = 0;

            var topicConfig = new TopicConfig();
            topicConfig["auto.offset.reset"] = "smallest";
            var config = new Config()
            {
                GroupId = "benchmark-consumer",
                DefaultTopicConfig = topicConfig
            };
            using (var consumer = new EventConsumer(config, broker))
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
