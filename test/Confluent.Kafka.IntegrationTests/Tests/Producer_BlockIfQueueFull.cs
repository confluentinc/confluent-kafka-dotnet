// Copyright 2018 Confluent Inc.
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

#pragma warning disable xUnit1026

using System;
using System.Text;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test dotnet.producer.block.if.queue.full = false throws
    ///     if queue fills up.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_BlockIfQueueFull(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", "doesntexistserver:9092" },
                { "dotnet.producer.block.if.queue.full", false },
                { "queue.buffering.max.messages", 2 }
            };

            // non-serializing Produce
            using (var producer = new Producer(producerConfig))
            {
                for (int i=0; i<2; ++i)
                {
                    producer.Produce(
                        (DeliveryReport msg) => {},
                        singlePartitionTopic, 0,
                        TestKey, 0, TestKey.Length,
                        TestValue, 0, TestValue.Length,
                        Timestamp.Default, null
                    );
                }
                Assert.Throws<KafkaException>(() => 
                    producer.Produce(
                        (DeliveryReport msg) => {},
                        singlePartitionTopic, 0,
                        TestKey, 0, TestKey.Length,
                        TestValue, 0, TestValue.Length,
                        Timestamp.Default, null
                    )
                );
            }

            // non-serializing ProduceAsync
            using (var producer = new Producer(producerConfig))
            {
                for (int i=0; i<2; ++i)
                {
                    producer.ProduceAsync(
                        singlePartitionTopic, 0,
                        TestKey, 0, TestKey.Length,
                        TestValue, 0, TestValue.Length,
                        Timestamp.Default, null
                    );
                }
                Assert.Throws<KafkaException>(() => 
                    producer.ProduceAsync(
                        singlePartitionTopic, 0,
                        TestKey, 0, TestKey.Length,
                        TestValue, 0, TestValue.Length,
                        Timestamp.Default, null
                    ).Wait()
                );
            }

            // serializing Produce
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for (int i=0; i<2; ++i)
                {
                    producer.Produce(
                        singlePartitionTopic, 
                        new Message<string, string> { Key = "hello", Value ="world" },
                        (DeliveryReport<string, string> msg) => {});
                }
                Assert.Throws<KafkaException>(() => 
                    producer.Produce( 
                        singlePartitionTopic, 
                        new Message<string, string> { Key = "hello", Value ="world" },
                    (DeliveryReport<string, string> msg) => {})
                );
            }

            // serializing ProduceAsync
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for (int i=0; i<2; ++i)
                {
                    producer.ProduceAsync(singlePartitionTopic, new Message<string, string> { Key = "hello", Value = "world" });
                }
                Assert.Throws<KafkaException>(() => 
                    producer.ProduceAsync(singlePartitionTopic, new Message<string, string> { Key = "hello", Value = "world" }).Wait()
                );
            }

        }
    }
}
