using System;
using System.Collections.Generic;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkConsumer
    {
        public static void BenchmarkConsumerImpl(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nTests, bool usePoll)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "u-bute-group" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer(consumerConfig))
            {
                for (var j=0; j<nTests; ++j)
                {
                    Console.WriteLine($"{consumer.Name} consuming from {topic} " + (usePoll ? "[Poll]" : "[Consume]"));

                    consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, firstMessageOffset) });

                    // consume 1 message before starting the timer to avoid including potential one-off delays.
                    Message msg;
                    consumer.Consume(out msg);

                    long startTime = DateTime.Now.Ticks;

                    if (usePoll)
                    {
                        int cnt = 0;
                        consumer.OnMessage += (_, m) => { cnt += 1; };

                        while (cnt < nMessages-1)
                        {
                            consumer.Poll(TimeSpan.FromSeconds(1));
                        }
                    }
                    else
                    {
                        var cnt = 0;

                        while (cnt < nMessages-1)
                        {
                            if (consumer.Consume(out msg, TimeSpan.FromSeconds(1)))
                            {
                                cnt += 1;
                            }
                        }
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Consumed {nMessages-1} in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{(nMessages-1) / (duration/10000.0):F0} messages/ms");
                }
            }
        }

        public static void Poll(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nTests)
            => BenchmarkConsumerImpl(bootstrapServers, topic, firstMessageOffset, nMessages, nTests, true);

        public static void Consume(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nTests)
            => BenchmarkConsumerImpl(bootstrapServers, topic, firstMessageOffset, nMessages, nTests, false);
    }
}
