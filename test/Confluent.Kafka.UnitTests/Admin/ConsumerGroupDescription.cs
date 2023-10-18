// Copyright 2023 Confluent Inc.
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

using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class ConsumerGroupDescriptionTests
    {
        [Fact]
        public void StringRepresentation()
        {
            // Coordinator null
            var description = new ConsumerGroupDescription
            {
                GroupId = "test",
                Error = ErrorCode.NoError,
                IsSimpleConsumerGroup = true,
                PartitionAssignor = "testAssignor",
                State = ConsumerGroupState.PreparingRebalance,
                Coordinator = null,
                Members = new List<MemberDescription>
                {
                    new MemberDescription
                    {
                        ClientId = "client1",
                        ConsumerId = "consumer1",
                        Host = "localhost",
                        Assignment = new MemberAssignment
                        {
                            TopicPartitions = new List<TopicPartition>
                            {
                                new TopicPartition("test1", 0),
                                new TopicPartition("test1", 1),
                            }
                        }
                    }
                },
                AuthorizedOperations = new List<AclOperation>
                {
                    AclOperation.Create
                }
            };
            Assert.Equal(
                @"{""GroupId"": ""test"", ""Error"": ""NoError"", ""IsSimpleConsumerGroup"": true"+
                @", ""PartitionAssignor"": ""testAssignor"", ""State"": ""PreparingRebalance"", ""Coordinator"": null" + 
                @", ""Members"": [{""ClientId"": ""client1"", ""GroupInstanceId"": null" + 
                @", ""ConsumerId"": ""consumer1"", ""Host"": ""localhost"", ""Assignment"": [{""Topic"": ""test1"", ""Partition"": 0}," +
                @"{""Topic"": ""test1"", ""Partition"": 1}]}], ""AuthorizedOperations"": [""Create""]}",
                description.ToString());

            // Coordinator not null, empty lists, GroupInstanceId, null AuthorizedOperations
            description = new ConsumerGroupDescription
            {
                GroupId = "test",
                Error = ErrorCode.NoError,
                IsSimpleConsumerGroup = true,
                PartitionAssignor = "testAssignor",
                State = ConsumerGroupState.PreparingRebalance,
                Coordinator = new Node
                {
                    Host = "localhost",
                    Port = 9092,
                    Id = 1,
                    Rack = null
                },
                Members = new List<MemberDescription>
                {
                    new MemberDescription
                    {
                        ClientId = "client1",
                        GroupInstanceId = "groupInstanceId1",
                        ConsumerId = "consumer1",
                        Host = "localhost",
                        Assignment = new MemberAssignment
                        {
                            TopicPartitions = new List<TopicPartition>
                            {}
                        }
                    }
                },
                AuthorizedOperations = null
            };
            Assert.Equal(
                @"{""GroupId"": ""test"", ""Error"": ""NoError"", ""IsSimpleConsumerGroup"": true"+
                @", ""PartitionAssignor"": ""testAssignor"", ""State"": ""PreparingRebalance"", ""Coordinator"": " + 
                @"{""Id"": 1, ""Host"": ""localhost"", ""Port"": 9092, ""Rack"": null}" +
                @", ""Members"": [{""ClientId"": ""client1"", ""GroupInstanceId"": ""groupInstanceId1""" + 
                @", ""ConsumerId"": ""consumer1"", ""Host"": ""localhost"", ""Assignment"": [" +
                @"]}], ""AuthorizedOperations"": null}",
                description.ToString());

        }
    }
}
