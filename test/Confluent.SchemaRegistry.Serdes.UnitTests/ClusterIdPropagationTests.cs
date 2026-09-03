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

using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests that the Schema Registry serdes report and accept the Kafka cluster
    ///     id, and that the extension methods resolve those capabilities on any
    ///     serializer or deserializer.
    /// </summary>
    public class ClusterIdPropagationTests : BaseSerializeDeserializeTests
    {
        private const string ClusterId = "lkc-resolved";

        private static List<KeyValuePair<string, string>> WithClusterId()
            => new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(
                    AssociatedNameStrategy.KafkaClusterIdConfig, "lkc-configured")
            };

        // The Associated strategy is the default, so a serde with no explicit
        // subject name strategy needs the cluster id.

        [Fact]
        public void AvroSerializer_NeedsClusterIdByDefault()
        {
            var serializer = new AvroSerializer<int>(schemaRegistryClient);

            Assert.True(serializer.NeedsClusterId());
        }

        [Fact]
        public void AvroDeserializer_NeedsClusterIdByDefault()
        {
            var deserializer = new AvroDeserializer<int>(schemaRegistryClient);

            Assert.True(deserializer.NeedsClusterId());
        }

        [Fact]
        public void JsonSerializer_NeedsClusterIdByDefault()
        {
            var serializer = new JsonSerializer<UnitTestJsonRecord>(schemaRegistryClient);

            Assert.True(serializer.NeedsClusterId());
        }

        [Fact]
        public void JsonDeserializer_NeedsClusterIdByDefault()
        {
            var deserializer = new JsonDeserializer<UnitTestJsonRecord>(schemaRegistryClient);

            Assert.True(deserializer.NeedsClusterId());
        }

        [Fact]
        public void AvroSerializer_DoesNotNeedClusterId_WhenConfigured()
        {
            var serializer = new AvroSerializer<int>(
                schemaRegistryClient, new AvroSerializerConfig(WithClusterId()));

            Assert.False(serializer.NeedsClusterId());
        }

        [Fact]
        public void AvroDeserializer_DoesNotNeedClusterId_WhenConfigured()
        {
            var deserializer = new AvroDeserializer<int>(
                schemaRegistryClient, new AvroDeserializerConfig(WithClusterId()));

            Assert.False(deserializer.NeedsClusterId());
        }

        [Fact]
        public void JsonSerializer_DoesNotNeedClusterId_WhenConfigured()
        {
            var serializer = new JsonSerializer<UnitTestJsonRecord>(
                schemaRegistryClient, new JsonSerializerConfig(WithClusterId()));

            Assert.False(serializer.NeedsClusterId());
        }

        [Fact]
        public void Serde_DoesNotNeedClusterId_WhenStrategyIsNotAssociated()
        {
            var serializer = new AvroSerializer<int>(
                schemaRegistryClient,
                new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic });

            Assert.False(serializer.NeedsClusterId());
        }

        [Fact]
        public void SetClusterId_SatisfiesTheNeed()
        {
            var serializer = new AvroSerializer<int>(schemaRegistryClient);
            Assert.True(serializer.NeedsClusterId());

            serializer.SetClusterId(ClusterId);

            Assert.False(serializer.NeedsClusterId());
        }

        [Fact]
        public void SetClusterId_SatisfiesTheNeed_OnADeserializer()
        {
            var deserializer = new AvroDeserializer<int>(schemaRegistryClient);
            Assert.True(deserializer.NeedsClusterId());

            deserializer.SetClusterId(ClusterId);

            Assert.False(deserializer.NeedsClusterId());
        }

        [Fact]
        public void SetClusterId_DoesNotOverrideAConfiguredValue()
        {
            var serializer = new AvroSerializer<int>(
                schemaRegistryClient, new AvroSerializerConfig(WithClusterId()));

            serializer.SetClusterId(ClusterId);

            Assert.False(serializer.NeedsClusterId());
        }

        // The extension methods must see through the sync-over-async adapter, since
        // that is how an async deserializer reaches a consumer.

        [Fact]
        public void Extensions_SeeThroughTheSyncOverAsyncAdapter()
        {
            IDeserializer<int> deserializer =
                new AvroDeserializer<int>(schemaRegistryClient).AsSyncOverAsync();

            Assert.True(deserializer.NeedsClusterId());

            deserializer.SetClusterId(ClusterId);

            Assert.False(deserializer.NeedsClusterId());
        }

        [Fact]
        public void Extensions_SeeThroughTheSyncOverAsyncSerializerAdapter()
        {
            ISerializer<int> serializer =
                new AvroSerializer<int>(schemaRegistryClient).AsSyncOverAsync();

            Assert.True(serializer.NeedsClusterId());

            serializer.SetClusterId(ClusterId);

            Assert.False(serializer.NeedsClusterId());
        }

        // A serde with no cluster id concept must report no need, and tolerate
        // being handed one anyway.

        [Fact]
        public void Extensions_ReportNoNeedForABuiltInSerializer()
        {
            Assert.False(Serializers.Utf8.NeedsClusterId());
        }

        [Fact]
        public void Extensions_ReportNoNeedForABuiltInDeserializer()
        {
            Assert.False(Deserializers.Utf8.NeedsClusterId());
        }

        [Fact]
        public void Extensions_SetClusterIdIsANoOpForABuiltInSerializer()
        {
            // Must not throw.
            Serializers.Utf8.SetClusterId(ClusterId);
            Deserializers.Utf8.SetClusterId(ClusterId);
        }

        [Fact]
        public void Extensions_DisposeIsANoOpForABuiltInSerializer()
        {
            // Must not throw, and must leave the serializer usable.
            Serializers.Utf8.Dispose();
            Deserializers.Utf8.Dispose();

            Assert.NotNull(Serializers.Utf8.Serialize("value",
                new SerializationContext(MessageComponentType.Value, "topic")));
        }
    }

    /// <summary>
    ///     A minimal record for the JSON serdes, which require a reference type.
    /// </summary>
    public class UnitTestJsonRecord
    {
        public int Value { get; set; }
    }
}
