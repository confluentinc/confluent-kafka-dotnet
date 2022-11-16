// Copyright 2020 Confluent Inc.
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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test committing an offset when not subscribed to a group. This is valid provided there 
        ///     are no active members of the group. If there is an active consumer group with the same
        ///     groupId, the commit would be rejected (typically with an UNKNOWN_MEMBER_ID or
        ///     ILLEGAL_GENERATION error).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Drain(string bootstrapServers)
        {
            LogToFile("start Consumer_Drain");

            int N = 142;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false,
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 100, N);
                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    var offsets = consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, 0), TimeSpan.FromSeconds(10));
                    Assert.Equal(0, offsets.Low);
                    Assert.Equal(N, offsets.High); // offsets.High is the next message to be read == the offset of last message + 1.
                    consumer.Commit(new[] { new TopicPartitionOffset(topic.Name, 0, new Offset(offsets.High)) });
                    consumer.Subscribe(topic.Name);
                    var cnt = 0;
                    while (consumer.Assignment.Count == 0)
                    {
                        Thread.Sleep(1000);
                        consumer.Consume(TimeSpan.FromSeconds(1));
                        Assert.True(cnt++ < 10);
                    }
                    var committed = consumer.Committed(TimeSpan.FromSeconds(10));
                    Assert.Single(committed);
                    Assert.Equal(N, committed[0].Offset);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Drain");
        }
    }
}
