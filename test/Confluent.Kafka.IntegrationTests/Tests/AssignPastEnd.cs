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
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test functionality of Consumer.Consume when assigned to offest
    ///     higher than the offset of the last message on a partition.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AssignPastEnd(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "u-bute-group" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                // Consume API
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+1) });
                Message msg;
                Assert.False(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));

                // Poll API
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+1) });
                consumer.OnMessage += (_, message) =>
                {
                    Assert.True(false);
                };
                consumer.Poll(TimeSpan.FromSeconds(10));
            }

        }

    }
}
