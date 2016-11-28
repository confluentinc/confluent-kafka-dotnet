using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
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

            public void SetDeliveryReport(MessageInfo deliveryReport)
            {
                if (--NumberOfMessages == 0)
                {
                    AutoEvent.Set();
                }
            }
        }

        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses custom delivery handler.
        /// </summary>
        [IntegrationTest(explicitExecutionRequired: true)]
        public static void BenchmarkProducerII(string bootstrapServers, string topic)
        {
            // mirrors the librdkafka performance test example.
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "queue.buffering.max.messages", 500000 },
                { "message.send.max.retries", 3 },
                { "retry.backoff.ms", 500 },
                { "session.timeout.ms", 6000 }
            };

            const int NUMBER_OF_TESTS = 5;
            const int NUMBER_OF_MESSAGES_TO_PRODUCE = 5000000;

            using (var producer = new Producer(config))
            {
                for (var j=0; j<NUMBER_OF_TESTS; ++j)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic}");

                    var deliveryHandler = new BenchmarkProducerDeliveryHandler(NUMBER_OF_MESSAGES_TO_PRODUCE);

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    var startTime = DateTime.Now.Ticks;
                    for (int i = 0; i < NUMBER_OF_MESSAGES_TO_PRODUCE; i++)
                    {
                        producer.ProduceAsync(topic, null, val, deliveryHandler);
                    }

                    deliveryHandler.AutoEvent.WaitOne();

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {NUMBER_OF_MESSAGES_TO_PRODUCE} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{NUMBER_OF_MESSAGES_TO_PRODUCE / (duration/10000.0):F0} messages/ms");
                }

                producer.Flush();
                Console.WriteLine("Disposing producer");
            }
        }

    }
}