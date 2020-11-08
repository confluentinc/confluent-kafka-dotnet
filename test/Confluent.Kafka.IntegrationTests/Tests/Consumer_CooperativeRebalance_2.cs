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
        ///     Test that lost partitions events are directed to the 
        ///     revoked handler if the lost partitions handler isn't 
        ///     specified.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_CooperativeRebalance_2(string bootstrapServers)
        {
            LogToFile("start Consumer_CooperativeRebalance_2");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                HeartbeatIntervalMs = 2000,
                SessionTimeoutMs = 6000, // minimum allowed by the broker.
                MaxPollIntervalMs = 6500
            };

            int assignCount = 0;
            int revokeCount = 0;

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetPartitionsAssignedHandler((c, p) => { assignCount += 1; })
                .SetPartitionsRevokedHandler((c, p) => { revokeCount += 1; })
                .Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic1.Name, 1, 1);

                consumer.Subscribe(topic1.Name);
                var cr1 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr1);
                Assert.Single(cr1.Message.Value);
                Assert.Equal(1, assignCount);

                // Exceed MaxPollIntervalMs
                Thread.Sleep(TimeSpan.FromSeconds(8));

                // The first call to Consume after MaxPollExceeded should result in an
                // exception reporting the problem.
                try
                {
                    consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException e)
                {
                    Assert.Equal(ErrorCode.Local_MaxPollExceeded, e.Error.Code);
                }

                // The second call should trigger the revoked handler (because a lost
                // handler has not been specified).
                Assert.Equal(0, revokeCount);
                consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Equal(1, revokeCount);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_CooperativeRebalance_2");
        }
    }
}
