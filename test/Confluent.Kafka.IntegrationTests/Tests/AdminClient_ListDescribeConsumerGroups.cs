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
            LogToFile("start AdminClient_ListDescribeConsumerGroups");
            var groupID = Guid.NewGuid().ToString();
            var nonExistentGroupID = Guid.NewGuid().ToString();
            const string clientID1 = "test.client.1";
            const string clientID2 = "test.client.2";

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig {
                BootstrapServers = bootstrapServers }).Build())
            {
                var listOptionsWithTimeout = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };
                var describeOptionsWithTimeout = new Admin.DescribeConsumerGroupsOptions()
                {
                    RequestTimeout = TimeSpan.FromSeconds(30),
                    IncludeAuthorizedOperations = true,
                };

                // We should not have any group initially.
                var groups = adminClient.ListConsumerGroupsAsync().Result;
                Assert.Empty(groups.Valid.Where(group => group.GroupId == groupID));
                Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupID));

                // Ensure that the partitioned topic we are using has exactly two partitions.
                Assert.Equal(2, partitionedTopicNumPartitions);

                // 1. One consumer group with one client.
                var consumerConfig = new ConsumerConfig
                {
                    GroupId = groupID,
                    BootstrapServers = bootstrapServers,
                    SessionTimeoutMs = 6000,
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                    ClientId = clientID1,

                };
                var consumer1 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                consumer1.Subscribe(new string[] { partitionedTopic });
                // Wait for rebalance.
                consumer1.Consume(TimeSpan.FromSeconds(10));

                groups = adminClient.ListConsumerGroupsAsync(listOptionsWithTimeout).Result;
                Assert.Single(groups.Valid.Where(group => group.GroupId == groupID));
                Assert.Empty(groups.Valid.Where(group => group.GroupId == nonExistentGroupID));
                var group = groups.Valid.Find(group => group.GroupId == groupID);
                Assert.Equal(ConsumerGroupState.Stable, group.State);
                Assert.False(group.IsSimpleConsumerGroup);

                var descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupID },
                    describeOptionsWithTimeout).Result;
                var groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID);
                var clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                clientIdToToppars[clientID1] = new List<TopicPartition>()
                {
                    new TopicPartition(partitionedTopic, 0),
                    new TopicPartition(partitionedTopic, 1),
                };
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Stable, "range", groupID, clientIdToToppars);

                // 2. One consumer group with two clients.
                consumerConfig.ClientId = clientID2;
                var consumer2 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                consumer2.Subscribe(new string[] { partitionedTopic });

                // Wait for rebalance.
                var state = ConsumerGroupState.PreparingRebalance;
                while (state != ConsumerGroupState.Stable)
                {
                    consumer1.Consume(TimeSpan.FromSeconds(1));
                    consumer2.Consume(TimeSpan.FromSeconds(1));

                    descResult = adminClient.DescribeConsumerGroupsAsync(
                        new List<String>() { groupID },
                        describeOptionsWithTimeout).Result;
                    Assert.Single(descResult.ConsumerGroupDescriptions.Where(group => group.GroupId == groupID));
                    groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID);
                    state = groupDesc.State;
                }

                clientIdToToppars[clientID1] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 0)
                };
                clientIdToToppars[clientID2] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 1)
                };
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Stable, "range", groupID, clientIdToToppars);

                // 3. Empty consumer group.
                consumer1.Close();
                consumer2.Close();
                consumer1.Dispose();
                consumer2.Dispose();


                // Check the 'States' option by listing Stable consumer groups, which shouldn't
                // include `groupID`.
                groups = adminClient.ListConsumerGroupsAsync(new Admin.ListConsumerGroupsOptions()
                { MatchStates = new List<ConsumerGroupState>() { ConsumerGroupState.Stable },
                  RequestTimeout = TimeSpan.FromSeconds(30) }).Result;
                Assert.Empty(groups.Valid.Where(group => group.GroupId == groupID));

                descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupID },
                    describeOptionsWithTimeout).Result;
                Assert.Single(descResult.ConsumerGroupDescriptions.Where(group => group.GroupId == groupID));
                groupDesc = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID);
                clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                checkConsumerGroupDescription(
                    groupDesc, ConsumerGroupState.Empty, "", groupID, clientIdToToppars);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_ListDescribeConsumerGroups");
        }
    }
}
