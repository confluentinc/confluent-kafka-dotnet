using System;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Producer benchmark masquarading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        [IntegrationTest(explicitExecutionRequired: true)]
        public static void BenchmarkProducer(string bootstrapServers, string topic)
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

                    byte cnt = 0;
                    var val = new byte[100].Select(a => ++cnt).ToArray();

                    var startTime = DateTime.Now.Ticks;
                    var tasks = new Task[NUMBER_OF_MESSAGES_TO_PRODUCE];
                    for (int i = 0; i < NUMBER_OF_MESSAGES_TO_PRODUCE; i++)
                    {
                        tasks[i] = producer.ProduceAsync(topic, null, val);
                    }

                    Task.WaitAll(tasks);
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