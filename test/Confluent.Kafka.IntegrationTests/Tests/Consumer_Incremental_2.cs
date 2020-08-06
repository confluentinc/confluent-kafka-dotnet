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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test sticky-cooperative assignor.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Incremental_2(string bootstrapServers)
        {
            LogToFile("start Consumer_Incremental_1");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                HeartbeatIntervalMs = 2000,
                SessionTimeoutMs = 10000,
                MaxPollIntervalMs = 10000,
            };

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetPartitionsAssignedHandler((c, p) => {
                    
                })
                .SetPartitionsRevokedHandler((c, p) => {

                })
                .SetPartitionsLostHandler((c, p) => {

                })
                .Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic1.Name, 1, 1);

                consumer.Subscribe(topic1.Name);
                var cr1 = consumer.Consume(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Incremental_1");
        }
    }
}
