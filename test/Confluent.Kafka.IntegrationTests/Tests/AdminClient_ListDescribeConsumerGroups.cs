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


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        // A convenience method to check the resultant GroupInfos obtained on describing a group.
        private void checkGroupInfo(
            GroupInfo gi, ConsumerGroupState stateCode, string protocol, string groupID, Dictionary<string, List<TopicPartition>> clientIdToToppars) 
        {
            Assert.Equal(groupID, gi.Group);
            Assert.Equal(ErrorCode.NoError, gi.Error.Code);
            Assert.Equal(stateCode, gi.StateCode);
            Assert.Equal(protocol, gi.Protocol);
            // We can't check exactly the Broker information, but we add a check for the zero-value of the Host.
            Assert.NotEqual("", gi.Broker.Host);
            Assert.Equal(clientIdToToppars.Count(), gi.Members.Count());
            // We will run all our tests on non-simple, consumer groups only.
            Assert.Equal("consumer", gi.ProtocolType);
            Assert.Equal(false, gi.IsSimpleConsumerGroup);

            foreach (var member in gi.Members)
            {
                Assert.True(clientIdToToppars.ContainsKey(member.ClientId));
                Assert.True(clientIdToToppars[member.ClientId].SequenceEqual(member.MemberAssignmentTopicPartitions));
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
                var describeOptionsWithTimeout = new Admin.DescribeConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };

                // We should not have any group initially.
                var groups = adminClient.ListConsumerGroups();
                Assert.Empty(groups.Where(group => group.Group == groupID));
                Assert.Empty(groups.Where(group => group.Group == nonExistentGroupID));

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

                groups = adminClient.ListConsumerGroups(listOptionsWithTimeout);
                Assert.Single(groups.Where(group => group.Group == groupID));
                Assert.Empty(groups.Where(group => group.Group == nonExistentGroupID));
                var group = groups.Find(group => group.Group == groupID);
                Assert.Equal(ConsumerGroupState.Stable, group.StateCode);
                Assert.Equal(false, group.IsSimpleConsumerGroup);

                groups = adminClient.DescribeConsumerGroups(new List<String>() { groupID }, describeOptionsWithTimeout);
                group = groups.Find(group => group.Group == groupID);
                var clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                clientIdToToppars[clientID1] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 0),
                    new TopicPartition(partitionedTopic, 1),
                };
                checkGroupInfo(group, ConsumerGroupState.Stable, "range", groupID, clientIdToToppars);

                // 2. One consumer group with two clients.
                consumerConfig.ClientId = clientID2;
                var consumer2 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                consumer2.Subscribe(new string[] { partitionedTopic });

                // Wait for rebalance.
                // TODO(milind): is this time enough or do we need to keep checking Stable by describing the group in
                // a loop?
                consumer2.Consume(TimeSpan.FromSeconds(10));

                groups = adminClient.DescribeConsumerGroups(new List<String>() { groupID }, describeOptionsWithTimeout);
                Assert.Single(groups.Where(group => group.Group == groupID));
                group = groups.Find(group => group.Group == groupID);
                clientIdToToppars[clientID1] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 0)
                };
                clientIdToToppars[clientID2] = new List<TopicPartition>() {
                    new TopicPartition(partitionedTopic, 1)
                };
                checkGroupInfo(group, ConsumerGroupState.Stable, "range", groupID, clientIdToToppars);

                // 3. Empty consumer group.
                consumer1.Close();
                consumer2.Close();
                consumer1.Dispose();
                consumer2.Dispose();

                // TODO(milind) do we need a wait here for rebalancing again?
                // Check the 'States' option by listing Stable consumer groups, which shouldn't
                // include `groupID`.
                groups = adminClient.ListConsumerGroups(new Admin.ListConsumerGroupsOptions() 
                { States = new List<ConsumerGroupState>() { ConsumerGroupState.Stable },
                  RequestTimeout = TimeSpan.FromSeconds(30) });
                Assert.Empty(groups.Where(group => group.Group == groupID));

                groups = adminClient.DescribeConsumerGroups(new List<String>() { groupID }, describeOptionsWithTimeout);
                Assert.Single(groups.Where(group => group.Group == groupID));
                group = groups.Find(group => group.Group == groupID);
                clientIdToToppars = new Dictionary<string, List<TopicPartition>>();
                checkGroupInfo(group, ConsumerGroupState.Empty, "", groupID, clientIdToToppars);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_ListDescribeConsumerGroups");
        }
    }
}
