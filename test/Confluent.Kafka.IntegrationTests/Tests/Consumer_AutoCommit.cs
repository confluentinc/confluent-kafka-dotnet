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
using System.Linq;
using System.Threading;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test auto commit operates as expected when set to false (that issue #362 is resolved).
        ///     note that 'default.topic.config' has been depreciated.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_AutoCommit(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_AutoCommit");

            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "auto.commit.interval.ms", 1000 },
                { "enable.auto.commit", false }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                bool done = false;
                consumer.OnPartitionEOF += (_, tpo)
                    => done = true;

                consumer.OnPartitionAssignment += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, firstProduced.Partition, firstProduced.Offset));
                };

                consumer.Subscribe(singlePartitionTopic);

                int msgCnt = 0;
                while (!done)
                {
                    ConsumeResult<Null, string> record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record != null)
                    {
                        msgCnt += 1;
                    }
                }

                Assert.Equal(msgCnt, N);

                Thread.Sleep(TimeSpan.FromSeconds(3));

                var committed = consumer.Committed(new [] { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));

                // if this was committing, would expect the committed offset to be first committed offset + N
                // (don't need to subtract 1 since the next message to be consumed is the value that is committed).
                Assert.NotEqual(firstProduced.Offset + N, committed[0].Offset);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_AutoCommit");
        }
    }
}
