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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test auto commit operates as expected when set to false (that issue #362 is resolved).
        ///     note that 'default.topic.config' has been deprecated.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_AutoCommit(string bootstrapServers)
        {
            LogToFile("start Consumer_AutoCommit");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                AutoCommitIntervalMs = 1000,
                EnableAutoCommit = false,
                EnablePartitionEof = true
            };

            using (var consumer =
                new ConsumerBuilder<Null, string>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Assert.Single(partitions);
                        return new List<TopicPartitionOffset> { new TopicPartitionOffset(singlePartitionTopic, firstProduced.Partition, firstProduced.Offset) };
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                int msgCnt = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }

                    msgCnt += 1;
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
