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

using Xunit;
using System;
using Confluent.Kafka.Admin;
using System.Collections.Generic;


namespace Confluent.Kafka.UnitTests
{
    public class DescribeTopicsErrorTests
    {
        private readonly List<DescribeTopicsOptions> options = new List<DescribeTopicsOptions>
        {
            new DescribeTopicsOptions {},
            new DescribeTopicsOptions { RequestTimeout = TimeSpan.FromMilliseconds(200) },
            new DescribeTopicsOptions { IncludeAuthorizedOperations = true },
            new DescribeTopicsOptions { RequestTimeout = TimeSpan.FromMilliseconds(200), IncludeAuthorizedOperations = false },
        };

        [Fact]
        public async void NullTopicCollection()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                foreach (var option in options)
                {
                    await Assert.ThrowsAsync<NullReferenceException>(() =>
                        adminClient.DescribeTopicsAsync(null, option)
                    );
                }
            }
        }
        
        [Fact]
        public async void EmptyTopicCollection()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                foreach (var option in options)
                {
                    var result = await adminClient.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames(new List<string> {}),
                        option);
                    Assert.Empty(result.TopicDescriptions);
                }
            }
        }

        
        [Fact]
        public async void WrongTopicNames()
        {
            var wrongTopicCollections = new List<TopicCollection>
            {
                TopicCollection.OfTopicNames(new List<string> {""}),
                TopicCollection.OfTopicNames(new List<string> {"correct", ""}),
            };
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                foreach (var option in options)
                {
                    foreach (var collection in wrongTopicCollections)
                    {
                        await Assert.ThrowsAsync<KafkaException>(() =>
                            adminClient.DescribeTopicsAsync(collection, option)
                        );
                    }
                }
            }
        }

        [Fact]
        public async void WrongRequestTimeoutValue()
        {
            var topicCollections =  TopicCollection.OfTopicNames(new List<string> {});
            var wrongRequestTimeoutValue = new DescribeTopicsOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(-1)
            };
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.DescribeTopicsAsync(topicCollections, wrongRequestTimeoutValue)
                );
            }
        }

        [Fact]
        public async void LocalTimeout()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            { 
                BootstrapServers = "localhost:90922",
                SocketTimeoutMs = 10
            }).Build())
            {
                foreach (var option in options)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.DescribeTopicsAsync(
                            TopicCollection.OfTopicNames(new List<string> {"test"}),
                            option)
                    );
                    Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
                }
            }
        }
    }
}
