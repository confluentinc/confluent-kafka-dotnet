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
    public class ListOffsetsErrorTests
    {
        private readonly List<ListOffsetsOptions> options = new List<ListOffsetsOptions>
        {
            new ListOffsetsOptions {},
            new ListOffsetsOptions { RequestTimeout = TimeSpan.FromMilliseconds(200) },
            new ListOffsetsOptions { IsolationLevel = IsolationLevel.ReadUncommitted },
            new ListOffsetsOptions { RequestTimeout = TimeSpan.FromMilliseconds(200), IsolationLevel = IsolationLevel.ReadCommitted },
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
                    adminClient.ListOffsetsAsync(
                        new List<TopicPartitionOffsetSpec> {},
                        new ListOffsetsOptions
                        {
                            RequestTimeout = TimeSpan.FromSeconds(-1)
                        })
                );
                Assert.Contains("expecting integer in range", ex.Message);
            }
        }

        [Fact]
        public async void InvalidPartitions()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            { 
                BootstrapServers = "localhost:90922",
                SocketTimeoutMs = 10
            }).Build())
            {
                foreach (var option in options)
                {
                    var invalidPartition = new List<TopicPartitionOffsetSpec>
                    {
                        new TopicPartitionOffsetSpec
                        {
                            TopicPartition = new TopicPartition(
                                "", 0),
                            OffsetSpec = OffsetSpec.Earliest()
                        }
                    };
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.ListOffsetsAsync(
                        invalidPartition,
                        option)
                    );
                    Assert.Contains("must be non-empty", ex.Message);

                    invalidPartition = new List<TopicPartitionOffsetSpec>
                    {
                        new TopicPartitionOffsetSpec
                        {
                            TopicPartition = new TopicPartition(
                                "correct", -1),
                            OffsetSpec = OffsetSpec.Earliest()
                        }
                    };
                    ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.ListOffsetsAsync(
                        invalidPartition,
                        option)
                    );
                    Assert.Contains("cannot be negative", ex.Message);
                }
            }
        }
        
        [Fact]
        public async void EmptyPartitions()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                foreach (var option in options)
                {
                    var result = await adminClient.ListOffsetsAsync(
                        new List<TopicPartitionOffsetSpec>
                        {},
                        option);
                    Assert.Empty(result.ResultInfos);
                }
            }
        }

        [Fact]
        public async void SamePartitionDifferentOffsets()
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
                        adminClient.ListOffsetsAsync(
                            new List<TopicPartitionOffsetSpec>
                            {
                                new TopicPartitionOffsetSpec
                                {
                                    TopicPartition = new TopicPartition(
                                        "test", 0),
                                    OffsetSpec = OffsetSpec.Earliest()
                                },
                                new TopicPartitionOffsetSpec
                                {
                                    TopicPartition = new TopicPartition(
                                        "test", 0),
                                    OffsetSpec = OffsetSpec.Latest()
                                }
                            },
                            option)
                    );
                    Assert.Contains("Partitions must not contain duplicates", ex.Message);
                }
            }
        }

        [Fact]
        public async void TwoDifferentPartitions()
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
                        adminClient.ListOffsetsAsync(
                            new List<TopicPartitionOffsetSpec>
                            {
                                new TopicPartitionOffsetSpec
                                {
                                    TopicPartition = new TopicPartition(
                                        "test", 0),
                                    OffsetSpec = OffsetSpec.Earliest()
                                },
                                new TopicPartitionOffsetSpec
                                {
                                    TopicPartition = new TopicPartition(
                                        "test", 1),
                                    OffsetSpec = OffsetSpec.Latest()
                                }
                            },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);
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
                        adminClient.ListOffsetsAsync(
                            new List<TopicPartitionOffsetSpec>
                            {
                                new TopicPartitionOffsetSpec
                                {
                                    TopicPartition = new TopicPartition(
                                        "test", 0),
                                    OffsetSpec = OffsetSpec.Earliest()
                                }
                            },
                            option)
                    );
                    Assert.Contains("Local: Timed out", ex.Message);
                }
            }
        }
    }
}
