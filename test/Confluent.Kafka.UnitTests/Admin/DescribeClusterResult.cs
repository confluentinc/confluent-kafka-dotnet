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
    public class DescribeClusterResultTests
    {
        [Fact]
        public void StringRepresentation()
        {
            var description = new DescribeClusterResult
            {
                ClusterId = "cluster",
                Controller = null,
                Nodes = new List<Node>(),
                AuthorizedOperations = new List<AclOperation>()
                {
                    AclOperation.Create
                },
            };
            Assert.Equal(
                @"{""ClusterId"": ""cluster"", ""Controller"": null, ""Nodes"": []" +
                @", ""AuthorizedOperations"": [""Create""]}",
                description.ToString());

            description = new DescribeClusterResult
            {
                ClusterId = "cluster",
                Controller = new Node
                {
                    Host = "host1",
                    Port = 9092,
                    Id = 3,
                    Rack = null
                },
                Nodes = new List<Node>
                {
                    new Node
                    {
                        Host = "host1",
                        Port = 9092,
                        Id = 3,
                        Rack = null
                    },
                    new Node
                    {
                        Host = "host2",
                        Port = 9093,
                        Id = 2,
                        Rack = null
                    },
                },
                AuthorizedOperations = null,
            };
            Assert.Equal(
                @"{""ClusterId"": ""cluster"", ""Controller"": {""Id"": 3, ""Host"": ""host1""" +
                @", ""Port"": 9092, ""Rack"": null}, ""Nodes"": [{""Id"": 3, ""Host"": ""host1""" +
                @", ""Port"": 9092, ""Rack"": null},{""Id"": 2, ""Host"": ""host2""" +
                @", ""Port"": 9093, ""Rack"": null}], ""AuthorizedOperations"": null}",
                description.ToString());
        }
    }
}
