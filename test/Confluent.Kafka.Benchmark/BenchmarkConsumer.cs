// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkConsumer
    {
        public static void BenchmarkConsumerImpl(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nTests, int nHeaders)
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = "benchmark-consumer-group",
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                ConsumeResultFields = nHeaders == 0 ? "none" : "headers"
            };

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, (topic_, data, isNull) => null, (topic_, data, isNull) => null))
            {
                for (var j=0; j<nTests; j += 1)
                {
                    Console.WriteLine($"{consumer.Name} consuming from {topic}");

                    consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, firstMessageOffset) });

                    // consume 1 message before starting the timer to avoid including potential one-off delays.
                    var record = consumer.Consume(TimeSpan.FromSeconds(10));

                    long startTime = DateTime.Now.Ticks;

                    var cnt = 0;

                    while (cnt < nMessages-1)
                    {
                        record = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (record != null)
                        {
                            cnt += 1;
                        }
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Consumed {nMessages-1} messages in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{(nMessages-1) / (duration/10000.0):F0}k msg/s");
                }
            }
        }

        public static void Consume(string bootstrapServers, string topic, long firstMessageOffset, int nMessages, int nHeaders, int nTests)
            => BenchmarkConsumerImpl(bootstrapServers, topic, firstMessageOffset, nMessages, nTests, nHeaders);
    }
}
