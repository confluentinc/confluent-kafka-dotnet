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
    public class DescribeTopicsResultTests
    {
        [Fact]
        public void StringRepresentation()
        {
            var description = new DescribeTopicsResult
            {
                TopicDescriptions = new List<TopicDescription>
                {
                    new TopicDescription
                    {
                        Name = "test1",
                        TopicId = new Uuid(2, 3),
                        IsInternal = false,
                        Error = new Error(ErrorCode.NoError),
                        Partitions = new List<TopicPartitionInfo>
                        {
                            new TopicPartitionInfo
                            {
                                Leader = new Node
                                {
                                    Host = "host1",
                                    Port = 9092,
                                    Id = 0,
                                    Rack = "rack2"
                                },
                                Partition = 0,
                                Replicas = new List<Node>
                                {
                                    new Node
                                    {
                                        Host = "host2",
                                        Port = 9092,
                                        Id = 0,
                                        Rack = null
                                    }
                                },
                                ISR = new List<Node>
                                {}
                            }
                        },
                        AuthorizedOperations = new List<AclOperation>
                        {
                            AclOperation.Create,
                            AclOperation.Describe,
                        }
                    },
                    new TopicDescription
                    {
                        Name = "test2",
                        TopicId = new Uuid(3, 2),
                        IsInternal = false,
                        Error = new Error(ErrorCode.UnknownTopicOrPart),
                        Partitions = new List<TopicPartitionInfo>
                        {
                            new TopicPartitionInfo
                            {
                                Leader = null,
                                Partition = 1,
                                Replicas = new List<Node>(),
                                ISR = new List<Node>
                                {
                                    new Node
                                    {
                                        Host = "host1",
                                        Port = 9093,
                                        Id = 2,
                                        Rack = "rack1"
                                    }
                                }
                            }
                        },
                        AuthorizedOperations = null
                    }
                }
            };
            
            var expectedString = @"{""TopicDescriptions"": [{""Name"": ""test1"", ""TopicId"": ""AAAAAAAAAAIAAAAAAAAAAw"", ""Error"": ""NoError"", ""IsInternal"": false" +
                @", ""Partitions"": [{""Partition"": 0, ""Leader"": {""Id"": 0, ""Host"": ""host1"", ""Port"": 9092, ""Rack"": ""rack2""}" +
                @", ""Replicas"": [{""Id"": 0, ""Host"": ""host2"", ""Port"": 9092, ""Rack"": null}], ""ISR"": []}]" +
                @", ""AuthorizedOperations"": [""Create"",""Describe""]}" +
                @",{""Name"": ""test2"", ""TopicId"": ""AAAAAAAAAAMAAAAAAAAAAg"", ""Error"": ""UnknownTopicOrPart"", ""IsInternal"": false" +
                @", ""Partitions"": [{""Partition"": 1, ""Leader"": null, ""Replicas"": []" +
                @", ""ISR"": [{""Id"": 2, ""Host"": ""host1"", ""Port"": 9093, ""Rack"": ""rack1""}]}]" +
                @", ""AuthorizedOperations"": null}]}";
            
            Assert.Equal(
                expectedString,
                description.ToString());

            var report = new DescribeTopicsReport
            {
                TopicDescriptions = description.TopicDescriptions
            };
            Assert.Equal(
                expectedString,
                report.ToString());
        }
    }
}
