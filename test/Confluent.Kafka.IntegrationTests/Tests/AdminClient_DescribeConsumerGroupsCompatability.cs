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
            ConsumerGroupDescription desc, ConsumerGroupState state, ConsumerGroupType type,
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
        ///     Test functionality of AdminClient.DescribeConsumerGroups.
        ///     We test three cases:
        ///     1. A list with two consumer groups created with new protocol.
        ///     2. A list with two consumer groups created with old protocol.
        ///     3. A list with four consumer groups, two created with new protocol and two with old protocol.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DescribeConsumerGroupsCompatability(string bootstrapServers)
        {
            if (TestConsumerGroupProtocol.IsClassic())
            {
                LogToFile("Creating new protocol Consumer Groups require" +
                          "Consumer Protocol");
                return;
            }

            LogToFile("start AdminClient_DescribeConsumerGroupsCompatability");
            var groupID_new1 = Guid.NewGuid().ToString();
            var groupID_new2 = Guid.NewGuid().ToString();
            var groupID_old1 = Guid.NewGuid().ToString();
            var groupID_old2 = Guid.NewGuid().ToString();
            const string clientID1 = "test.client.1";
            const string clientID2 = "test.client.2";
            const string clientID3 = "test.client.3";
            const string clientID4 = "test.client.4";

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                var describeOptionsWithTimeout = new Admin.DescribeConsumerGroupsOptions()
                {
                    RequestTimeout = TimeSpan.FromSeconds(30),
                    IncludeAuthorizedOperations = true,
                };

                var consumerConfigs = new List<(string GroupId, string ClientId, string ProtocolType)>
                {
                    (groupID_new1, clientID1, "consumer"),
                    (groupID_new2, clientID2, "consumer"),
                    (groupID_old1, clientID3, "classic"),
                    (groupID_old2, clientID4, "classic")
                };

                var consumers = new List<IConsumer<byte[], byte[]>>();

                foreach (var config in consumerConfigs)
                {
                    var consumerConfig = new ConsumerConfig
                    {
                        GroupId = config.GroupId,
                        BootstrapServers = bootstrapServers,
                        SessionTimeoutMs = 6000,
                        PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                        ClientId = config.ClientId,
                        GroupProtocolType = config.ProtocolType
                    };
                
                    var consumer = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                    consumer.Subscribe(new[] { partitionedTopic });

                    // Wait for rebalance
                    consumer.Consume(TimeSpan.FromSeconds(10));

                    consumers.Add(consumer);
                };

                var groupIdToClientIdToToppars = new Dictionary<string, Dictionary<string, List<TopicPartition>>>();

                // Group IDs and Client IDs
                var groupClientMapping = new Dictionary<string, string>
                {
                    { groupID_new1, clientID1 },
                    { groupID_new2, clientID2 },
                    { groupID_old1, clientID3 },
                    { groupID_old2, clientID4 }
                };

                // Loop through each group ID and client ID
                foreach (var entry in groupClientMapping)
                {
                    var groupID = entry.Key;
                    var clientID = entry.Value;

                    // Create the topic partition list for this client ID
                    var topicPartitions = new List<TopicPartition>
                    {
                        new TopicPartition(partitionedTopic, 0),
                        new TopicPartition(partitionedTopic, 1),
                    };

                    // Add the mapping to the nested dictionary
                    if (!groupIdToClientIdToToppars.ContainsKey(groupID))
                    {
                        groupIdToClientIdToToppars[groupID] = new Dictionary<string, List<TopicPartition>>();
                    }

                    groupIdToClientIdToToppars[groupID][clientID] = topicPartitions;
                }

                // We test for 3 scenarios, passing the two groups with new protocl in first,
                // and the two groups with old protocol in the second and then a mix of both.
                // List with two consumer groups created with new protocol.
                var descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupID_new1, groupID_new2 },
                    describeOptionsWithTimeout).Result;
                var groupDesc1 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_new1);
                var groupDesc2 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_new2);

                checkConsumerGroupDescription(
                    groupDesc1, ConsumerGroupState.Stable, ConsumerGroupType.Consumer, "range", groupID_new1, groupIdToClientIdToToppars[groupID_new1]);
                checkConsumerGroupDescription(
                    groupDesc2, ConsumerGroupState.Stable, ConsumerGroupType.Consumer, "range", groupID_new2, groupIdToClientIdToToppars[groupID_new2]);
                
                // List with two consumer groups created with old protocol.
                descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupID_old1, groupID_old2 },
                    describeOptionsWithTimeout).Result;
                groupDesc1 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_old1);
                groupDesc2 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_old2);

                checkConsumerGroupDescription(
                    groupDesc1, ConsumerGroupState.Stable, ConsumerGroupType.Classic, "range", groupID_old1, groupIdToClientIdToToppars[groupID_old1]);
                checkConsumerGroupDescription(
                    groupDesc2, ConsumerGroupState.Stable, ConsumerGroupType.Classic, "range", groupID_old2, groupIdToClientIdToToppars[groupID_old2]);
                
                // List with four consumer groups, two created with new protocol and two with old protocol.
                descResult = adminClient.DescribeConsumerGroupsAsync(
                    new List<String>() { groupID_new1, groupID_old1, groupID_new2, groupID_old2 },
                    describeOptionsWithTimeout).Result;
                groupDesc1 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_new1);
                groupDesc2 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_old1);
                var groupDesc3 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_new2);
                var groupDesc4 = descResult.ConsumerGroupDescriptions.Find(group => group.GroupId == groupID_old2);

                checkConsumerGroupDescription(
                    groupDesc1, ConsumerGroupState.Stable, ConsumerGroupType.Consumer, "range", groupID_new1, groupIdToClientIdToToppars[groupID_new1]);
                checkConsumerGroupDescription(
                    groupDesc2, ConsumerGroupState.Stable, ConsumerGroupType.Classic, "range", groupID_old1, groupIdToClientIdToToppars[groupID_old1]);
                checkConsumerGroupDescription(
                    groupDesc3, ConsumerGroupState.Stable, ConsumerGroupType.Consumer, "range", groupID_new2, groupIdToClientIdToToppars[groupID_new2]);
                checkConsumerGroupDescription(
                    groupDesc4, ConsumerGroupState.Stable, ConsumerGroupType.Classic, "range", groupID_old2, groupIdToClientIdToToppars[groupID_old2]);    


                foreach (var consumer in consumers)
                {
                    consumer.Close();
                    consumer.Dispose();
                }

            }
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DescribeConsumerGroupsCompatability");
        }
    }
}
