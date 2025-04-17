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
using System.Threading.Tasks;

namespace Confluent.Kafka.UnitTests
{
    public class ElectLeadersErrorTests
    { 
        private AdminClientConfig GetTestConfig()
        {
            return new AdminClientConfig
            {
                BootstrapServers = "localhost:90922",  
                SocketTimeoutMs = 30                   
            };
        }

        [Theory]
        [InlineData(ElectionType.Preferred)]
        [InlineData(ElectionType.Unclean)]
        public async Task ElectLeadersAsync_EmptyPartitions_ThrowsKafkaException(ElectionType electionType)
        {
            using var adminClient = new AdminClientBuilder(GetTestConfig()).Build();

            var exception = await Assert.ThrowsAsync<KafkaException>(() =>
                adminClient.ElectLeadersAsync(
                    electionType,
                    new List<TopicPartition>(),
                    new ElectLeadersOptions())
            );

            Assert.Contains("Timed out", exception.Message);
        }

        [Theory]
        [InlineData(ElectionType.Preferred)]
        [InlineData(ElectionType.Unclean)]
        public async Task ElectLeadersAsync_NullPartitions_ThrowsKafkaException(ElectionType electionType)
        {
            using var adminClient = new AdminClientBuilder(GetTestConfig()).Build();

            var exception = await Assert.ThrowsAsync<KafkaException>(() =>
                adminClient.ElectLeadersAsync(
                    electionType,
                    null,
                    new ElectLeadersOptions())
            );

            Assert.Contains("Timed out", exception.Message);
        }

        [Theory]
        [InlineData(ElectionType.Preferred)]
        [InlineData(ElectionType.Unclean)]
        public async Task ElectLeadersAsync_DuplicatePartitions_ThrowsKafkaException(ElectionType electionType)
        {
            using var adminClient = new AdminClientBuilder(GetTestConfig()).Build();

            var exception = await Assert.ThrowsAsync<KafkaException>(() =>
                adminClient.ElectLeadersAsync(
                    electionType,
                    new List<TopicPartition> { new TopicPartition("topic", 0), new TopicPartition("topic", 0)},
                    new ElectLeadersOptions())
            );

            Assert.Contains("Duplicate partitions specified", exception.Message);
        }

        [Theory]
        [InlineData(ElectionType.Preferred)]
        [InlineData(ElectionType.Unclean)]
        public async Task ElectLeadersAsync_ValidRequest_TimesOut(ElectionType electionType)
        {
            using var adminClient = new AdminClientBuilder(GetTestConfig()).Build();

            var exception = await Assert.ThrowsAsync<KafkaException>(() =>
                adminClient.ElectLeadersAsync(
                    electionType,
                    new List<TopicPartition> { new TopicPartition("topic", 0) },
                    new ElectLeadersOptions { RequestTimeout = TimeSpan.FromMilliseconds(1000) })
            );

            Assert.Contains("Timed out", exception.Message);
        }
    }
}