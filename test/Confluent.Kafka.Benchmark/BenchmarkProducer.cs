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

            public void HandleDeliveryReport(Message deliveryReport)
            {
                if (--NumberOfMessages == 0)
                {
                    AutoEvent.Set();
                }
            }
        }

        private static long BenchmarkProducerImpl(string bootstrapServers, string topic, int nMessages, int nTests, bool useDeliveryHandler)
        {
            // mirrors the librdkafka performance test example.
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "queue.buffering.max.messages", 2000000 },
                { "message.send.max.retries", 3 },
                { "retry.backoff.ms", 500 }
            };

            Message firstDeliveryReport = default(Message);

            using (var producer = new Producer(config))
            {
                for (var j=0; j<nTests; ++j)
                {
                    Console.WriteLine($"{producer.Name} producing on {topic} " + (useDeliveryHandler ? "[DeliveryHandler]" : "[Task]"));

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    // this avoids including connection setup, topic creation time, etc.. in result.
                    firstDeliveryReport = producer.ProduceAsync(topic, null, val).Result;

                    var startTime = DateTime.Now.Ticks;

                    if (useDeliveryHandler)
                    {
                        var deliveryHandler = new BenchmarkProducerDeliveryHandler(nMessages);

                        for (int i = 0; i < nMessages; i++)
                        {
                            producer.ProduceAsync(topic, null, val, deliveryHandler);
                        }

                        deliveryHandler.AutoEvent.WaitOne();
                    }
                    else
                    {
                        var tasks = new Task[nMessages];
                        for (int i = 0; i < nMessages; i++)
                        {
                            tasks[i] = producer.ProduceAsync(topic, null, val);
                        }
                        Task.WaitAll(tasks);
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {nMessages} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{nMessages / (duration/10000.0):F0} messages/ms");
                }

                producer.Flush();
            }

            return firstDeliveryReport.Offset;
        }

        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static long TaskProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, false);

        /// <summary>
        ///     Producer benchmark (with custom delivery handler) masquarading
        ///     as an integration test. Uses Task based produce method.
        /// </summary>
        public static long DeliveryHandlerProduce(string bootstrapServers, string topic, int nMessages, int nTests)
            => BenchmarkProducerImpl(bootstrapServers, topic, nMessages, nTests, true);
    }
}
