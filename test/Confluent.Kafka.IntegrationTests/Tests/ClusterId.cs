// Copyright 2026 Confluent Inc.
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
using System.Diagnostics;
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
        ///     A producer hands a cluster id aware serializer a resolver that yields
        ///     the id of the cluster it is connected to.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterIdPropagation(string bootstrapServers)
        {
            LogToFile("start ClusterIdPropagation");

            var expectedClusterId = ExpectedClusterId(bootstrapServers);

            var aware = new ClusterIdAwareSerializer();
            using (new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetValueSerializer(aware)
                    .Build())
            {
                Assert.Equal(expectedClusterId, aware.ClusterIdResolver());
            }

            // Key and value serializers each receive the resolver exactly once.
            var key = new ClusterIdAwareSerializer();
            var value = new ClusterIdAwareSerializer();
            using (new TestProducerBuilder<string, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetKeySerializer(key)
                    .SetValueSerializer(value)
                    .Build())
            {
                Assert.Equal(expectedClusterId, key.ClusterIdResolver());
                Assert.Equal(expectedClusterId, value.ClusterIdResolver());
            }
            Assert.Equal(1, key.SetClusterIdResolverCallCount);
            Assert.Equal(1, value.SetClusterIdResolverCallCount);

            // A producer sharing another producer's handle serves its serializers
            // too.
            var dependent = new ClusterIdAwareSerializer();
            using (var main = new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (new DependentProducerBuilder<Null, string>(main.Handle)
                    .SetValueSerializer(dependent)
                    .Build())
            {
                Assert.Equal(expectedClusterId, dependent.ClusterIdResolver());
            }

            LogToFile("end   ClusterIdPropagation");
        }

        /// <summary>
        ///     Constructing a producer does not wait on the cluster: the resolver
        ///     is handed over, and it is the serializer that pays for resolving the
        ///     id, when it first needs it.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterIdResolutionIsDeferred(string bootstrapServers)
        {
            LogToFile("start ClusterIdResolutionIsDeferred");

            var aware = new ClusterIdAwareSerializer();
            var stopwatch = Stopwatch.StartNew();

            // No broker listens here, so resolving the id during construction
            // would block until its timeout.
            using (new TestProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = "localhost:1" })
                    .SetValueSerializer(aware)
                    .Build())
            {
            }

            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10),
                $"Producer construction took {stopwatch.Elapsed}");
            Assert.NotNull(aware.ClusterIdResolver);

            LogToFile("end   ClusterIdResolutionIsDeferred");
        }

        /// <summary>
        ///     A consumer hands a cluster id aware deserializer a resolver that
        ///     yields the id of the cluster it is connected to, including through a
        ///     deserializer builder.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClusterIdPropagationConsumer(string bootstrapServers)
        {
            LogToFile("start ClusterIdPropagationConsumer");

            var expectedClusterId = ExpectedClusterId(bootstrapServers);

            var aware = new ClusterIdAwareDeserializer();
            using (new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                })
                    .SetValueDeserializer(aware)
                    .Build())
            {
                Assert.Equal(expectedClusterId, aware.ClusterIdResolver());
            }

            // A deserializer reached through a builder is served too, and is
            // disposed along with the consumer that built it.
            var built = new ClusterIdAwareDeserializer();
            using (new TestConsumerBuilder<Null, string>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString()
                })
                    .SetValueDeserializerBuilder(new StubDeserializerBuilder(built))
                    .Build())
            {
                Assert.Equal(expectedClusterId, built.ClusterIdResolver());
            }
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
            public Func<string> ClusterIdResolver { get; private set; }
            public int SetClusterIdResolverCallCount { get; private set; }
            public bool Disposed { get; private set; }

            public void SetClusterIdResolver(Func<string> clusterIdResolver)
            {
                ++SetClusterIdResolverCallCount;
                ClusterIdResolver = clusterIdResolver;
            }

            public void DisposeOwnedResources()
                => Disposed = true;

            public byte[] Serialize(string data, SerializationContext context)
                => Serializers.Utf8.Serialize(data, context);
        }

        private class ClusterIdAwareDeserializer
            : IDeserializer<string>, IClusterIdAware, ISerdeOwnedResources
        {
            public Func<string> ClusterIdResolver { get; private set; }
            public bool Disposed { get; private set; }

            public void SetClusterIdResolver(Func<string> clusterIdResolver)
                => ClusterIdResolver = clusterIdResolver;

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
