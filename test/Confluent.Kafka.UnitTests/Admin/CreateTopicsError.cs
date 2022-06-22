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

using Xunit;
using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.UnitTests
{
    public class CreateTopicsErrorTests
    {
        [Fact]
        public async void NullTopic()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:666" }).Build())
            {
                // null topics argument
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.CreateTopicsAsync(null)
                );
            }
        }

        [Fact]
        public async void LocalTimeout()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:666" }).Build())
            {
                var testTopics = new List<TopicSpecification>
                {
                    new TopicSpecification
                    {
                        Name = "my-topic",
                        ReplicationFactor = 1,
                        NumPartitions = 1
                    },
                    new TopicSpecification
                    {
                        Name = "my-topic-2",
                        ReplicationFactor = 1,
                        NumPartitions = 1
                    }
                };

                var options = new CreateTopicsOptions
                {
                    RequestTimeout = TimeSpan.FromMilliseconds(200)
                };

                foreach (var topic in testTopics)
                {
                    // Correct input, fail with timeout
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                        adminClient.CreateTopicsAsync(
                            new List<TopicSpecification>{topic},
                            options)
                    );
                    Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
                }
            }
        }
    }
}
