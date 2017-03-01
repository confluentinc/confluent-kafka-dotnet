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
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic DeserializingConsumer test (consume mode).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DeserializingConsumer_Consume(string bootstrapServers, string topic, string partitionedTopic)
        {
            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, topic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "api.version.request", true }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                bool done = false;

                consumer.OnPartitionEOF += (_, partition)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Equal(1, partitions.Count);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(topic);

                int msgCnt = 0;
                while (!done)
                {
                    Message<Null, string> msg;
                    if (consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
                    {
                        Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                        Assert.True(Math.Abs((DateTime.UtcNow - msg.Timestamp.DateTime).TotalMinutes) < 1.0);
                        msgCnt += 1;
                    }
                }

                Assert.Equal(msgCnt, N);
            }
        }

    }
}
