// Copyright 2025 Confluent Inc.
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
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that ClusterId returns a non-null, non-empty string after a
        ///     broker connection has been established on Producer, Consumer, and
        ///     AdminClient.
        ///
        ///     ClusterId wraps rd_kafka_clusterid() with timeout_ms=0, which
        ///     returns the cached value. A metadata request is used to ensure the
        ///     cluster id has been fetched and cached before reading the property.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterId(string bootstrapServers)
        {
            LogToFile("start ClusterId");

            var adminClientConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                // Metadata request populates the cached cluster id.
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                var clusterId = adminClient.ClusterId;
                Assert.NotNull(clusterId);
                Assert.NotEmpty(clusterId);
            }

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            using (var producer = new TestProducerBuilder<Null, string>(producerConfig).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                var clusterId = producer.ClusterId;
                Assert.NotNull(clusterId);
                Assert.NotEmpty(clusterId);
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000
            };
            using (var consumer = new TestConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var adminClient = new DependentAdminClientBuilder(consumer.Handle).Build())
            {
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                var clusterId = consumer.ClusterId;
                Assert.NotNull(clusterId);
                Assert.NotEmpty(clusterId);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   ClusterId");
        }
    }
}
