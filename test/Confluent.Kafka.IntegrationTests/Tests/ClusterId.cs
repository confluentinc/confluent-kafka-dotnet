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
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.TestsCommon;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Every client type reports the cluster id of the cluster it is
        ///     connected to, and agrees with the admin DescribeCluster API.
        /// </summary>
        /// <remarks>
        ///     Assumes broker v0.10.0 or higher, which is when the cluster id was
        ///     added to the metadata response.
        /// </remarks>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task ClusterId(string bootstrapServers)
        {
            LogToFile("start ClusterId");

            var timeout = TimeSpan.FromSeconds(30);

            using (var producer = new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var consumer = new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                }).Build())
            using (var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var producerClusterId = producer.ClusterId(timeout);
                var consumerClusterId = consumer.ClusterId(timeout);
                var adminClusterId = adminClient.ClusterId(timeout);

                Assert.False(string.IsNullOrEmpty(producerClusterId));
                Assert.Equal(producerClusterId, consumerClusterId);
                Assert.Equal(producerClusterId, adminClusterId);

                // Cross-check against the independent admin API, so a marshalling
                // fault could not pass as a plausible-looking value.
                var describeResult = await adminClient.DescribeClusterAsync();
                Assert.Equal(describeResult.ClusterId, producerClusterId);

                // Repeated calls must each release the native string.
                for (int i = 0; i < 50; ++i)
                {
                    Assert.Equal(producerClusterId, adminClient.ClusterId(timeout));
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   ClusterId");
        }

        /// <summary>
        ///     A producer resolves the cluster id and supplies it to a serializer
        ///     that asks for one, and does not ask the cluster at all when no
        ///     serializer needs it.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterIdPropagation(string bootstrapServers)
        {
            LogToFile("start ClusterIdPropagation");

            var expectedClusterId = ExpectedClusterId(bootstrapServers);

            // A serializer that needs the cluster id is given it.
            var needy = new ClusterIdAwareSerializer(needsClusterId: true);
            using (new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetValueSerializer(needy)
                    .Build())
            {
            }
            Assert.Equal(expectedClusterId, needy.ClusterId);

            // A serializer that does not need it is left alone.
            var indifferent = new ClusterIdAwareSerializer(needsClusterId: false);
            using (new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetValueSerializer(indifferent)
                    .Build())
            {
            }
            Assert.Null(indifferent.ClusterId);

            // Both key and value serializers are served by a single resolution.
            var key = new ClusterIdAwareSerializer(needsClusterId: true);
            var value = new ClusterIdAwareSerializer(needsClusterId: true);
            using (new TestProducerBuilder<string, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetKeySerializer(key)
                    .SetValueSerializer(value)
                    .Build())
            {
            }
            Assert.Equal(expectedClusterId, key.ClusterId);
            Assert.Equal(expectedClusterId, value.ClusterId);
            Assert.Equal(1, key.SetClusterIdCallCount);
            Assert.Equal(1, value.SetClusterIdCallCount);

            LogToFile("end   ClusterIdPropagation");
        }

        /// <summary>
        ///     A consumer resolves the cluster id and supplies it to a deserializer
        ///     that asks for one, including through a serializer builder.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterIdPropagationConsumer(string bootstrapServers)
        {
            LogToFile("start ClusterIdPropagationConsumer");

            var expectedClusterId = ExpectedClusterId(bootstrapServers);

            var needy = new ClusterIdAwareDeserializer(needsClusterId: true);
            using (new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                })
                    .SetValueDeserializer(needy)
                    .Build())
            {
            }
            Assert.Equal(expectedClusterId, needy.ClusterId);

            var indifferent = new ClusterIdAwareDeserializer(needsClusterId: false);
            using (new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                })
                    .SetValueDeserializer(indifferent)
                    .Build())
            {
            }
            Assert.Null(indifferent.ClusterId);

            // A deserializer reached through a builder is served too, and is
            // disposed along with the consumer that built it.
            var built = new ClusterIdAwareDeserializer(needsClusterId: true);
            using (new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                })
                    .SetValueDeserializerBuilder(new StubDeserializerBuilder(built))
                    .Build())
            {
            }
            Assert.Equal(expectedClusterId, built.ClusterId);
            Assert.True(built.Disposed);

            LogToFile("end   ClusterIdPropagationConsumer");
        }

        private static string ExpectedClusterId(string bootstrapServers)
        {
            using (var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                return adminClient.ClusterId(TimeSpan.FromSeconds(30));
            }
        }

        private class ClusterIdAwareSerializer
            : ISerializer<string>, IClusterIdAware, ISerdeOwnedResources
        {
            private readonly bool needsClusterId;

            public ClusterIdAwareSerializer(bool needsClusterId)
                => this.needsClusterId = needsClusterId;

            public string ClusterId { get; private set; }
            public int SetClusterIdCallCount { get; private set; }
            public bool Disposed { get; private set; }

            public bool NeedsClusterId
                => needsClusterId && ClusterId == null;

            public void SetClusterId(string clusterId)
            {
                ++SetClusterIdCallCount;
                ClusterId = clusterId;
            }

            public void DisposeOwnedResources()
                => Disposed = true;

            public byte[] Serialize(string data, SerializationContext context)
                => Serializers.Utf8.Serialize(data, context);
        }

        private class ClusterIdAwareDeserializer
            : IDeserializer<string>, IClusterIdAware, ISerdeOwnedResources
        {
            private readonly bool needsClusterId;

            public ClusterIdAwareDeserializer(bool needsClusterId)
                => this.needsClusterId = needsClusterId;

            public string ClusterId { get; private set; }
            public bool Disposed { get; private set; }

            public bool NeedsClusterId
                => needsClusterId && ClusterId == null;

            public void SetClusterId(string clusterId)
                => ClusterId = clusterId;

            public void DisposeOwnedResources()
                => Disposed = true;

            public string Deserialize(
                ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                => Deserializers.Utf8.Deserialize(data, isNull, context);
        }

        private class StubDeserializerBuilder : IDeserializerBuilder<string>
        {
            private readonly IDeserializer<string> deserializer;

            public StubDeserializerBuilder(IDeserializer<string> deserializer)
                => this.deserializer = deserializer;

            public IDeserializer<string> Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => deserializer;
        }
    }
}
