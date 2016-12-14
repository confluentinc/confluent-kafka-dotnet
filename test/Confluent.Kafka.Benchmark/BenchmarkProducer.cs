using System;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkProducer
    {
        private class BenchmarkProducerDeliveryHandler : IDeliveryHandler
        {
            public int NumberOfMessages { get; private set; }
            public AutoResetEvent AutoEvent { get; private set; }

            public BenchmarkProducerDeliveryHandler(int numberOfMessages)
            {
                this.NumberOfMessages = numberOfMessages;
                this.AutoEvent = new AutoResetEvent(false);
            }

            public bool MarshalData { get { return false; } }

            public void HandleDeliveryReport(MessageInfo deliveryReport)
            {
                if (--NumberOfMessages == 0)
                {
                    AutoEvent.Set();
                }
            }
        }

        private static void BenchmarkProducerImpl(string bootstrapServers, string topic, bool useDeliveryHandler)
        {
            // mirrors the librdkafka performance test example.
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "queue.buffering.max.messages", 500000 },
                { "message.send.max.retries", 3 },
                { "retry.backoff.ms", 500 }
            };

            const int NUMBER_OF_TESTS = 5;
            const int NUMBER_OF_MESSAGES_TO_PRODUCE = 5000000;

            using (var producer = new Producer(config))
            {
                for (var j=0; j<NUMBER_OF_TESTS; ++j)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic}");

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    // this is here to avoid calculating connection setup, topic creation, etc..
                    producer.ProduceAsync(topic, null, val).Wait();

                    var startTime = DateTime.Now.Ticks;

                    if (useDeliveryHandler)
                    {
                        var deliveryHandler = new BenchmarkProducerDeliveryHandler(NUMBER_OF_MESSAGES_TO_PRODUCE);

                        for (int i = 0; i < NUMBER_OF_MESSAGES_TO_PRODUCE; i++)
                        {
                            producer.ProduceAsync(topic, null, val, deliveryHandler);
                        }

                        deliveryHandler.AutoEvent.WaitOne();
                    }
                    else
                    {
                        var tasks = new Task[NUMBER_OF_MESSAGES_TO_PRODUCE];
                        for (int i = 0; i < NUMBER_OF_MESSAGES_TO_PRODUCE; i++)
                        {
                            tasks[i] = producer.ProduceAsync(topic, null, val);
                        }
                        Task.WaitAll(tasks);
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {NUMBER_OF_MESSAGES_TO_PRODUCE} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{NUMBER_OF_MESSAGES_TO_PRODUCE / (duration/10000.0):F0} messages/ms");
                }

                producer.Flush();
                Console.WriteLine("Disposing producer");
            }
        }

        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static void TaskProduce(string bootstrapServers, string topic)
            => BenchmarkProducerImpl(bootstrapServers, topic, false);

        /// <summary>
        ///     Producer benchmark (with custom delivery handler) masquarading
        ///     as an integration test. Uses Task based produce method.
        /// </summary>
        public static void DeliveryHandlerProduce(string bootstrapServers, string topic)
            => BenchmarkProducerImpl(bootstrapServers, topic, true);
    }
}
