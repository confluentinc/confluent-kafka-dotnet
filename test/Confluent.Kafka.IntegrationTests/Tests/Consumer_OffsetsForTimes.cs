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
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
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
            const int N = 10;
            const int Partition = 0;

            var messages = ProduceMessages(bootstrapServers, singlePartitionTopic, Partition, N);

            var consumerConfig = new Dictionary<string, object>
            {
                {"group.id", Guid.NewGuid().ToString()},
                {"bootstrap.servers", bootstrapServers}
            };

            var firstMessage = messages[0];
            var lastMessage = messages[N - 1];
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                // NOTE: When calling OffsetsForTimes a proper timeout for must be set. 
                // If it will be too short, we'll get an exception here or incorrect result.
                // See librdkafka implementation for details https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.c#L2475
                var timeout = TimeSpan.FromSeconds(10);

                // Getting the offset for the first produced message timestamp
                var result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(firstMessage.TopicPartition, firstMessage.Timestamp) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(result[0].Offset, firstMessage.Offset);
                Assert.False(result[0].Error.HasError);

                // Getting the offset for the last produced message timestamp
                result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(lastMessage.TopicPartition, lastMessage.Timestamp) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(result[0].Offset, lastMessage.Offset);
                Assert.False(result[0].Error.HasError);

                // Getting the offset for the timestamp that very far in the past
                var unixTimeEpoch = Timestamp.UnixTimeEpoch;
                result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(new TopicPartition(singlePartitionTopic, Partition), new Timestamp(unixTimeEpoch, TimestampType.CreateTime)) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(0, result[0].Offset);
                Assert.False(result[0].Error.HasError);

                // Getting the offset for the timestamp that very far in the future
                result = consumer.OffsetsForTimes(
                        new[] { new TopicPartitionTimestamp(new TopicPartition(singlePartitionTopic, Partition), new Timestamp(int.MaxValue, TimestampType.CreateTime)) },
                        timeout)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(0, result[0].Offset);
                Assert.False(result[0].Error.HasError);
            }
        }

        private static Message<string, string>[] ProduceMessages(string bootstrapServers, string topic, int partition, int count)
        {
            var producerConfig = new Dictionary<string, object>
            {
                {"bootstrap.servers", bootstrapServers}
            };

            var messages = new Message<string, string>[count];
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for (var index = 0; index < count; index++)
                {
                    var message = producer.ProduceAsync(topic, partition, $"test key {index}", $"test val {index}", Timestamp.Default, null).Result;
                    messages[index] = message;
                    Task.Delay(200).Wait();
                }
            }

            return messages;
        }
    }
}
