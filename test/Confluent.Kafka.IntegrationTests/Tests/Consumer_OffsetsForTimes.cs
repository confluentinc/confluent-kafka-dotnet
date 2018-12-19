﻿// Copyright 2016-2017 Confluent Inc.
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
using System.Text;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic OffsetsForTimes test on Consumer.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_OffsetsForTimes(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_OffsetsForTimes");

            const int N = 10;
            const int Partition = 0;

            var messages = ProduceMessages(bootstrapServers, singlePartitionTopic, Partition, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers
            };

            var firstMessage = messages[0];
            var lastMessage = messages[N - 1];
            using (var consumer = new Consumer(consumerConfig))
            {
                var timeout = TimeSpan.FromSeconds(10);

                // If empty request, expect empty result.
                var result = consumer.OffsetsForTimes(new TopicPartitionTimestamp[0], timeout).ToList();
                Assert.Empty(result);

                // Getting the offset for the first produced message timestamp
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

                // Getting the offset for the timestamp that is very far in the past
                var unixTimeEpoch = Timestamp.UnixTimeEpoch;
                result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(new TopicPartition(singlePartitionTopic, Partition), new Timestamp(unixTimeEpoch, TimestampType.CreateTime)) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(0, result[0].Offset);

                // Getting the offset for the timestamp that very far in the future
                result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(new TopicPartition(singlePartitionTopic, Partition), new Timestamp(int.MaxValue, TimestampType.CreateTime)) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(0, result[0].Offset);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_OffsetsForTimes");
        }

        private static DeliveryResult[] ProduceMessages(string bootstrapServers, string topic, int partition, int count)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var messages = new DeliveryResult[count];
            using (var producer = new Producer(producerConfig))
            {
                for (var index = 0; index < count; index++)
                {
                    var message = producer.ProduceAsync(
                        new TopicPartition(topic, partition),
                        new Message
                        { 
                            Key = Serializers.Utf8.Serialize($"test key {index}", true, null, null),
                            Value = Serializers.Utf8.Serialize($"test val {index}", true, null, null),
                            Timestamp = Timestamp.Default, 
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
