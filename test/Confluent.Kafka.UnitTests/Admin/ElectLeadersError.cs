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

using Xunit;
using System;
using Confluent.Kafka.Admin;
using System.Collections.Generic;

namespace Confluent.Kafka.UnitTests
{
    public class ElectLeadersErrorTests
    {
        private readonly List<ElectLeadersOptions> options = new List<ElectLeadersOptions>
        {
            new ElectLeadersOptions {},
            new ElectLeadersOptions { RequestTimeout = TimeSpan.FromMilliseconds(200) },
            new ElectLeadersOptions { OperationTimeout = TimeSpan.FromMilliseconds(200)},
            new ElectLeadersOptions { RequestTimeout = TimeSpan.FromMilliseconds(200), OperationTimeout = TimeSpan.FromMilliseconds(200) },
        };

        [Fact]
        public async void InvalidRequestTimeout()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            { 
                BootstrapServers = "localhost:90922",
                SocketTimeoutMs = 10
            }).Build())
            {
                var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.ElectLeadersAsync(
                        new ElectLeadersRequest { ElectionType = ElectionType.Preferred, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0) } },
                        new ElectLeadersOptions
                        {
                            RequestTimeout = TimeSpan.FromSeconds(-1)
                        })
                );
                Assert.Contains("expecting integer in range", ex.Message);
                ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.ElectLeadersAsync(
                        new ElectLeadersRequest { ElectionType = ElectionType.Unclean, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0) } },
                        new ElectLeadersOptions
                        {
                            RequestTimeout = TimeSpan.FromSeconds(-1)
                        })
                );
                Assert.Contains("expecting integer in range", ex.Message);
            }
        }

        [Fact]
        public async void EmptyPartitions()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            { 
                BootstrapServers = "localhost:90922",
                SocketTimeoutMs = 10
            }).Build())
            {
                foreach(var option in options)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Preferred, Partitions = new List<TopicPartition> { } },
                            option)
                    );
                    Assert.Contains("No partitions specified", ex.Message);

                    ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Unclean, Partitions = new List<TopicPartition> { } },
                            option)
                    );
                    Assert.Contains("No partitions specified", ex.Message);
                }
            }
        }

        [Fact]
        public async void DuplicatePartitions()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            { 
                BootstrapServers = "localhost:90922",
                SocketTimeoutMs = 10
            }).Build())
            {
                foreach(var option in options)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Preferred, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0), new TopicPartition("topic", 0) } },
                            option)
                    );
                    Assert.Contains("Duplicate partitions specified", ex.Message);

                    ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Unclean, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0), new TopicPartition("topic", 0) } },
                            option)
                    );
                    Assert.Contains("Duplicate partitions specified", ex.Message);
                }
            }
        }

        [Fact]
        public async void SinglePartition()
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
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Preferred, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0) } },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);

                    ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Unclean, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0) } },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);
                }
            }
        }

        [Fact]
        public async void MultiplePartitions()
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
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Preferred, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0), new TopicPartition("topic", 1) } },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);

                    ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.ElectLeadersAsync(
                            new ElectLeadersRequest { ElectionType = ElectionType.Unclean, Partitions = new List<TopicPartition> { new TopicPartition("topic", 0), new TopicPartition("topic", 1) } },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);
                }
            }
        }
    }
}