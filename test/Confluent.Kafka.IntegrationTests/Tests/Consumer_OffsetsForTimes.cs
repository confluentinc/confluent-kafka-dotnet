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
        public static async Task Consumer_OffsetsForTimes(string bootstrapServers, string topic, string partitionedTopic)
        {
            const int N = 10;

            var producerConfig = new Dictionary<string, object>
            {
                {"bootstrap.servers", bootstrapServers},
                {"api.version.request", true}
            };

            var messages = new Message<string, string>[N];
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for (int index = 0; index < N; index++)
                {
                    var message = await producer.ProduceAsync(topic, $"test key {index}", $"test val {index}");
                    messages[index] = message;
                }
            }

            var consumerConfig = new Dictionary<string, object>
            {
                {"group.id", Guid.NewGuid().ToString()},
                {"bootstrap.servers", bootstrapServers}, 
                {"api.version.request", true}
            };

            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                // A proper timeout must be set. If it will be too short, we'll get an error or incorrect result here.
                // See librdkafka implementation for details https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.c#L2433
                var result = consumer.OffsetsForTimes(
                        new[] {new TopicPartitionTimestamp(messages[0].TopicPartition, messages[0].Timestamp)},
                        TimeSpan.FromSeconds(10))
                    .ToList();

                Assert.Equal(result.Count, 1);
                Assert.Equal(result[0].Offset, messages[0].Offset);

                result = consumer.OffsetsForTimes(
                        new[] {new TopicPartitionTimestamp(messages[N - 1].TopicPartition, messages[N - 1].Timestamp)},
                        TimeSpan.FromSeconds(10))
                    .ToList();

                Assert.Equal(result.Count, 1);
                Assert.Equal(result[0].Offset, messages[N - 1].Offset);
            }
        }
    }
}