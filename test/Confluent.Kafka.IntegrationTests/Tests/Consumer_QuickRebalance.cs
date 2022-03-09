// Copyright 2022 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        private static IConsumer<Null, string> quickRebalanceCreateConsumer(
            String bootstrapServers, String groupId)
        {
            return new ConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true,
                    EnableAutoOffsetStore = false,
                    AutoCommitIntervalMs = 500,
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin,
                })
                .SetPartitionsAssignedHandler((c, partitions) => { })
                .SetPartitionsRevokedHandler((c, partitions) => { })
                .Build();
        }

        /// <summary>
        ///     Test consumer behavior in a scenario where the group quickly rebalances.
        ///     The test is just that execution should complete (not hang).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_QuickRebalance(string bootstrapServers)
        {
            LogToFile("start Consumer_QuickRebalance");

            var groupId = Guid.NewGuid().ToString();
            var numPartitions = 4;
            using (var topic = new TemporaryTopic(bootstrapServers, numPartitions))
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 100, 1000);

                var consumer1 = quickRebalanceCreateConsumer(bootstrapServers, groupId);
                consumer1.Subscribe(topic.Name);
                ConsumeResult<Null, String> cr = null;
                while (cr == null || cr.Message == null)
                {
                    cr = consumer1.Consume(TimeSpan.FromSeconds(2));
                }
                var consumer2 = quickRebalanceCreateConsumer(bootstrapServers, groupId);
                consumer2.Subscribe(topic.Name);
                cr = null;
                while (cr == null || cr.Message == null)
                {
                    // Should not get stuck in this loop.
                    cr = consumer2.Consume(TimeSpan.FromSeconds(2));
                }
                consumer1.Close();
                consumer2.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_QuickRebalance");
        }
    }
}
