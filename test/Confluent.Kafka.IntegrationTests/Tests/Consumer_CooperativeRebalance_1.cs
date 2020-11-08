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
using System.Collections.Generic;
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test cooperative-sticky assignor and various aspects
        ///     of the .NET incremental rebalancing API.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_CooperativeRebalance_1(string bootstrapServers)
        {
            LogToFile("start Consumer_CooperativeRebalance_1");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                HeartbeatIntervalMs = 2000,
                SessionTimeoutMs = 6000,
                MaxPollIntervalMs = 6500,
            };

            int assignCount = 0;
            int revokeCount = 0;
            int lostCount = 0;

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetPartitionsAssignedHandler((c, p) => {
                    assignCount += 1;
                    Assert.Single(p);
                })
                .SetPartitionsRevokedHandler((c, p) => {
                    revokeCount += 1;
                })
                .SetPartitionsLostHandler((c, p) => {
                    lostCount += 1;
                })
                .Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic1.Name, 1, 1);

                consumer.Subscribe(topic1.Name);
                var cr1 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr1);
                Assert.Single(cr1.Message.Value);

                // Subscribe to a second one partition topic, the second assign
                // call should be incremental (checked in the handler).
                consumer.Subscribe(new List<string> { topic1.Name, topic2.Name });

                Util.ProduceNullStringMessages(bootstrapServers, topic2.Name, 1, 1);
                var cr2 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr2);
                Assert.Single(cr2.Message.Value);

                Assert.Equal(2, assignCount);
                Assert.Equal(0, lostCount);
                Assert.Equal(0, revokeCount);

                var cr3 = consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr3);

                // Exceed MaxPollIntervalMs => lost partitions.
                Thread.Sleep(TimeSpan.FromSeconds(8));

                consumer.Close();
            }

            Assert.Equal(2, assignCount);
            Assert.Equal(1, lostCount);
            Assert.Equal(0, revokeCount);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_CooperativeRebalance_1");
        }
    }
}
