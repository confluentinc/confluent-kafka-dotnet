// Copyright 2024 Confluent Inc.
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
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Admin;
using Confluent.Kafka.TestsCommon;
using System.Threading;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.ListConsumerGroups.
        ///     
        ///     We test the following situations:
        ///      1. when creating a consumer of type T in group G and listing with that type,
        ///         all returned groups should be of type T and G must be included.
        ///      2. when creating a consumer of type T and listing with the opposite type,
        ///         all returned groups should be of type opposite of T and G must not be included.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_ListConsumerGroups(string bootstrapServers)
        {
            var usedType = ConsumerGroupType.Consumer;
            var oppositeType = ConsumerGroupType.Classic;
            if (TestConsumerGroupProtocol.IsClassic())
            {
                usedType = ConsumerGroupType.Classic;
                oppositeType = ConsumerGroupType.Consumer;
            }

            var listOptionsWithUsed = new ListConsumerGroupsOptions()
            {
                RequestTimeout = TimeSpan.FromSeconds(30),
                MatchTypes = new List<ConsumerGroupType> { usedType },
            };
            var listOptionsWithOpposite = new ListConsumerGroupsOptions()
            {
                RequestTimeout = TimeSpan.FromSeconds(30),
                MatchTypes = new List<ConsumerGroupType> { oppositeType },
            };

            var groupId = Guid.NewGuid().ToString();
            const string clientId = "test.client";
            LogToFile("start AdminClient_ListConsumerGroups");

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {

            var consumerConfig = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                ClientId = clientId,
            };
            
            using (var consumer =
                new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                try
                {
                    consumer.Subscribe(new string[] { partitionedTopic });
                    // Wait for rebalance.
                    consumer.Consume(TimeSpan.FromSeconds(10));

                    Thread.Sleep(2000);

                    // Our consumer group should be present with same group type option
                    var groups = adminClient.ListConsumerGroupsAsync(listOptionsWithUsed).Result;
                    Assert.Empty(groups.Valid.Where(group => group.Type != usedType));
                    Assert.Single(groups.Valid.Where(group => group.GroupId == groupId));
                    
                    var group = groups.Valid.Find(group => group.GroupId == groupId);
                    Assert.Equal(ConsumerGroupState.Stable, group.State);
                    Assert.False(group.IsSimpleConsumerGroup);

                    // Our consumer group should not be present with opposite group type option
                    groups = adminClient.ListConsumerGroupsAsync(listOptionsWithOpposite).Result;
                    Assert.Empty(groups.Valid.Where(group => group.Type != oppositeType));
                    Assert.Empty(groups.Valid.Where(group => group.GroupId == groupId));
                }
                finally
                {
                    consumer.Close();
                }
            }

            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end AdminClient_ListConsumerGroups");
        }
    }
}
