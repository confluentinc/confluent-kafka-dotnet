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
using System.Linq;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Admin;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        // A convenience method to check the resultant ConsumerGroupDescription obtained on describing a group.
        private void checkConsumerGroupDescription(
            ConsumerGroupDescription desc, ConsumerGroupState state,
            string protocol, string groupID,
            Dictionary<string, List<TopicPartition>> clientIdToToppars)
        {
            Assert.Equal(groupID, desc.GroupId);
            Assert.Equal(ErrorCode.NoError, desc.Error.Code);
            Assert.Equal(state, desc.State);
            Assert.Equal(protocol, desc.PartitionAssignor);
            // We can't check exactly the Broker information, but we add a
            // check for the zero-value of the Host.
            Assert.NotEqual("", desc.Coordinator.Host);
            Assert.Equal(clientIdToToppars.Count(), desc.Members.Count());
            // We will run all our tests on non-simple consumer groups only.
            Assert.False(desc.IsSimpleConsumerGroup);
            Assert.NotEmpty(desc.AuthorizedOperations);

            foreach (var member in desc.Members)
            {
                Assert.True(clientIdToToppars.ContainsKey(member.ClientId));
                Assert.True(clientIdToToppars[member.ClientId].SequenceEqual(member.Assignment.TopicPartitions));
            }
        }

        /// <summary>
        ///     Test functionality of AdminClient.ListConsumerGroups and
        ///     AdminClient.DescribeConsumerGroups. We test three cases:
        ///     1. One consumer group with one client.
        ///     2. One consumer group with two clients.
        ///     3. Empty consumer group.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_ListDescribeConsumerGroups(string bootstrapServers)
        {
            var groupId = Guid.NewGuid().ToString();
            var nonExistentGroupId = Guid.NewGuid().ToString();
            if (!TestConsumerGroupProtocol.IsClassic())
            {
                LogToFile("start AdminClient_ListDescribeConsumerGroups with Consumer Protocol");
                const string clientId = "test.client";

                // Create an AdminClient here - we need it throughout the test.
                using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = bootstrapServers
                }).Build())
                {
                    var listOptionsWithTimeout = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };
                    var listOptionsWithClassic = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30), MatchGroupTypes = new List<ConsumerGroupType> { ConsumerGroupType.Classic } };
                    var listOptionsWithConsumer = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30), MatchGroupTypes = new List<ConsumerGroupType> { ConsumerGroupType.Consumer } };
                    // We should not have any group initially.
                    var groups = adminClient.ListConsumerGroupsAsync().Result;
                    Assert.Empty(groups.Valid.Where(group => group.GroupId == groupId));
                    Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupId));

                    // Ensure that the partitioned topic we are using has exactly two partitions.
                    Assert.Equal(2, partitionedTopicNumPartitions);

                    var consumerConfig = new ConsumerConfig
                    {
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        SessionTimeoutMs = 6000,
                        PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                        ClientId = clientId,

                    };
                    var consumer = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                    consumer.Subscribe(new string[] { partitionedTopic });
                    // Wait for rebalance.
                    consumer.Consume(TimeSpan.FromSeconds(10));

                    // Our Consumer Group should not be present with Classic Type option
                    groups = adminClient.ListConsumerGroupsAsync(listOptionsWithClassic).Result;
                    Assert.Empty(groups.Valid.Where(group => group.GroupType != ConsumerGroupType.Classic));
                    Assert.Empty(groups.Valid.Where(group => group.GroupId == groupId));

                    // Our Consumer Group should be present with Consumer Type option
                    groups = adminClient.ListConsumerGroupsAsync(listOptionsWithConsumer).Result;
                    Assert.Empty(groups.Valid.Where(group => group.GroupType != ConsumerGroupType.Consumer));
                    Assert.Single(groups.Valid.Where(group => group.GroupId == groupId));
                    Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupId));

                    var group = groups.Valid.Find(group => group.GroupId == groupId);
                    Assert.Equal(ConsumerGroupState.Stable, group.State);
                    Assert.False(group.IsSimpleConsumerGroup);

                    consumer.Close();
                    consumer.Dispose();
                }

                Assert.Equal(0, Library.HandleCount);
                LogToFile("end   AdminClient_ListDescribeConsumerGroups with Consumer Protocol");
                return;
            }

            LogToFile("start AdminClient_ListDescribeConsumerGroups");
            const string clientID1 = "test.client.1";
            const string clientID2 = "test.client.2";

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                var listOptionsWithTimeout = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };
                var describeOptionsWithTimeout = new Admin.DescribeConsumerGroupsOptions()
                {
                    RequestTimeout = TimeSpan.FromSeconds(30),
                    IncludeAuthorizedOperations = true,
                };

                // We should not have any group initially.
                var groups = adminClient.ListConsumerGroupsAsync().Result;
                Assert.Empty(groups.Valid.Where(group => group.GroupId == groupId));
                Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupId));

                // Ensure that the partitioned topic we are using has exactly two partitions.
                Assert.Equal(2, partitionedTopicNumPartitions);

                // 1. One consumer group with one client.
                var consumerConfig = new ConsumerConfig
                {
                    GroupId = groupId,
                    BootstrapServers = bootstrapServers,
                    SessionTimeoutMs = 6000,
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                    ClientId = clientID1,

                };
                var consumer1 = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                consumer1.Subscribe(new string[] { partitionedTopic });
                // Wait for rebalance.
                consumer1.Consume(TimeSpan.FromSeconds(10));

                groups = adminClient.ListConsumerGroupsAsync(listOptionsWithTimeout).Result;
                Assert.Single(groups.Valid.Where(group => group.GroupId == groupId));
                Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupId));
                var group = groups.Valid.Find(group => group.GroupId == groupId);
                Assert.Equal(ConsumerGroupState.Stable, group.State);
                Assert.False(group.IsSimpleConsumerGroup);

                var descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupId },
                    describeOptionsWithTimeout).Result;
                var groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupId);
                var clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                clientIdToToppars[clientID1] = new List<TopicPartition>()
                {
                    new TopicPartition(partitionedTopic, 0),
                    new TopicPartition(partitionedTopic, 1),
                };
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Stable, "range", groupId, clientIdToToppars);

                // 2. One consumer group with two clients.
                consumerConfig.ClientId = clientID2;
                var consumer2 = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                consumer2.Subscribe(new string[] { partitionedTopic });

                // Wait for rebalance.
                var state = ConsumerGroupState.PreparingRebalance;
                while (state != ConsumerGroupState.Stable)
                {
                    consumer1.Consume(TimeSpan.FromSeconds(1));
                    consumer2.Consume(TimeSpan.FromSeconds(1));

                    descResult = adminClient.DescribeConsumerGroupsAsync(
                        new List<String>() { groupId },
                        describeOptionsWithTimeout).Result;
                    Assert.Single(descResult.ConsumerGroupDescriptions.Where(group => group.GroupId == groupId));
                    groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupId);
                    state = groupDesc.State;
                }

                clientIdToToppars[clientID1] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 0)
                };
                clientIdToToppars[clientID2] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 1)
                };
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Stable, "range", groupId, clientIdToToppars);

                // 3. Empty consumer group.
                consumer1.Close();
                consumer2.Close();
                consumer1.Dispose();
                consumer2.Dispose();


                // Check the 'States' option by listing Stable consumer groups, which shouldn't
                // include `groupID`.
                groups = adminClient.ListConsumerGroupsAsync(new Admin.ListConsumerGroupsOptions()
                {
                    MatchStates = new List<ConsumerGroupState>() { ConsumerGroupState.Stable },
                    RequestTimeout = TimeSpan.FromSeconds(30)
                }).Result;
                Assert.Empty(groups.Valid.Where(group => group.GroupId == groupId));

                descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupId },
                    describeOptionsWithTimeout).Result;
                Assert.Single(descResult.ConsumerGroupDescriptions.Where(group => group.GroupId == groupId));
                groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupId);
                clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Empty, "", groupId, clientIdToToppars);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_ListDescribeConsumerGroups");
        }
    }
}
