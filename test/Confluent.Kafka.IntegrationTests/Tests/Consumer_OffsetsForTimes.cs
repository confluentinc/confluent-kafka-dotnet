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

#pragma warning disable xUnit1026

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Basic OffsetsForTimes test on Consumer.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_OffsetsForTimes(string bootstrapServers)
        {
            LogToFile("start Consumer_OffsetsForTimes");

            const int N = 10;
            const int Partition = 0;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                var messages = ProduceMessages(bootstrapServers, topic.Name, Partition, N);

                var firstMessage = messages[0];
                var lastMessage = messages[N - 1];
                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    var timeout = TimeSpan.FromSeconds(10);

                    // If empty request, expect empty result.
                    var result = consumer.OffsetsForTimes(new TopicPartitionTimestamp[0], timeout).ToList();
                    Assert.Empty(result);
                    
                    // Getting the offset for the first produced message timestamp.
                    result = consumer.OffsetsForTimes(
                            new[] { new TopicPartitionTimestamp(firstMessage.TopicPartition, firstMessage.Timestamp) },
                            timeout)
                        .ToList();

                    Assert.Single(result);
                    Assert.Equal(result[0].Offset, firstMessage.Offset);

                    // Getting the offset for the last produced message timestamp
                    result = consumer.OffsetsForTimes(
                            new[] { new TopicPartitionTimestamp(lastMessage.TopicPartition, lastMessage.Timestamp) },
                            timeout)
                        .ToList();

                    Assert.Single(result);
                    Assert.Equal(result[0].Offset, lastMessage.Offset);

                    // Getting the offset for a timestamp that is very far in the past.
                    var unixTimeEpoch = Timestamp.UnixTimeEpoch;
                    result = consumer.OffsetsForTimes(
                            new[] { new TopicPartitionTimestamp(new TopicPartition(topic.Name, Partition), new Timestamp(100, TimestampType.CreateTime)) },
                            timeout)
                        .ToList();
                    Assert.Single(result);
                    // According to the Java documentation which states: "The returned offset for each partition is
                    // the earliest offset whose timestamp is greater than or equal to the given timestamp in the
                    // corresponding partition." this is technically incorrect, but this is what is returned by the
                    // broker.
                    Assert.Equal(0, result[0].Offset);

                    // Getting the offset for an timestamp that is very far in the future.
                    result = consumer.OffsetsForTimes(
                            new[] { new TopicPartitionTimestamp(new TopicPartition(topic.Name, Partition), new Timestamp(long.MaxValue, TimestampType.CreateTime)) },
                            timeout)
                        .ToList();

                    Assert.Single(result);
                    Assert.Equal(Offset.End, result[0].Offset); // Offset.End == -1
                }
            }

            // Empty topic case
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                var result = consumer.OffsetsForTimes(
                    new List<TopicPartitionTimestamp> { new TopicPartitionTimestamp(topic.Name, 0, new Timestamp(10000, TimestampType.CreateTime)) },
                    TimeSpan.FromSeconds(30));

                Assert.Single(result);
                Assert.Equal(Offset.End, result[0].Offset); // Offset.End == -1
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_OffsetsForTimes");
        }

        private static DeliveryResult<byte[], byte[]>[] ProduceMessages(string bootstrapServers, string topic, int partition, int count)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var baseTime = 100000;
            var messages = new DeliveryResult<byte[], byte[]>[count];
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                for (var index = 0; index < count; ++index)
                {
                    var message = producer.ProduceAsync(
                        new TopicPartition(topic, partition),
                        new Message<byte[], byte[]>
                        { 
                            Key = Serializers.Utf8.Serialize($"test key {index}", SerializationContext.Empty),
                            Value = Serializers.Utf8.Serialize($"test val {index}", SerializationContext.Empty),
                            Timestamp = new Timestamp(baseTime + index*1000, TimestampType.CreateTime),
                            Headers = null
                        }
                    ).Result;
                    messages[index] = message;
                    Task.Delay(200).Wait();
                }
            }

            return messages;
        }
    }
}
