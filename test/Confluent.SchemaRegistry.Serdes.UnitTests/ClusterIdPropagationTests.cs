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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests that the Schema Registry serdes accept a Kafka cluster id resolver
    ///     and use it for association lookups, that the extension methods reach it
    ///     on any serializer or deserializer, and that the Associated strategy
    ///     invokes the resolver lazily.
    /// </summary>
    public class ClusterIdPropagationTests : BaseSerializeDeserializeTests
    {
        private const string ClusterId = "lkc-resolved";
        private const string ConfiguredClusterId = "lkc-configured";

        private static List<KeyValuePair<string, string>> WithClusterId()
            => new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(
                    AssociatedNameStrategy.KafkaClusterIdConfig, ConfiguredClusterId)
            };

        // The Associated strategy is the default, so a serde with no explicit
        // subject name strategy resolves the cluster id on first use and registers
        // under the subject associated with that cluster.

        [Fact]
        public async Task AvroSerializer_ResolvesTheClusterIdOnFirstSerialize()
        {
            Associate(ClusterId, "resolved-subject");

            int calls = 0;
            var serializer = new AvroSerializer<int>(schemaRegistryClient);
            serializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });
            Assert.Equal(0, calls);

            await serializer.SerializeAsync(1, ValueContext(testTopic));
            await serializer.SerializeAsync(2, ValueContext(testTopic));

            Assert.Equal(1, calls);
            Assert.True(subjectStore.ContainsKey("resolved-subject"));
        }

        [Fact]
        public async Task AvroDeserializer_ResolvesTheClusterIdOnFirstDeserialize()
        {
            Associate(ClusterId, "resolved-subject");

            var bytes = await new AvroSerializer<int>(schemaRegistryClient,
                new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic })
                .SerializeAsync(1, ValueContext(testTopic));

            int calls = 0;
            var deserializer = new AvroDeserializer<int>(schemaRegistryClient);
            deserializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });
            Assert.Equal(0, calls);

            Assert.Equal(1, await deserializer.DeserializeAsync(bytes, false, ValueContext(testTopic)));
            Assert.Equal(1, await deserializer.DeserializeAsync(bytes, false, ValueContext(testTopic)));

            Assert.Equal(1, calls);
        }

        [Fact]
        public async Task JsonSerializer_ResolvesTheClusterIdOnFirstSerialize()
        {
            Associate(ClusterId, "resolved-subject");

            int calls = 0;
            var serializer = new JsonSerializer<UnitTestJsonRecord>(schemaRegistryClient);
            serializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });
            Assert.Equal(0, calls);

            await serializer.SerializeAsync(new UnitTestJsonRecord { Value = 1 }, ValueContext(testTopic));

            Assert.Equal(1, calls);
            Assert.True(subjectStore.ContainsKey("resolved-subject"));
        }

        [Fact]
        public async Task Serde_AConfiguredClusterId_WinsOverTheResolver()
        {
            Associate(ConfiguredClusterId, "configured-subject");
            Associate(ClusterId, "resolved-subject");

            int calls = 0;
            var serializer = new AvroSerializer<int>(
                schemaRegistryClient, new AvroSerializerConfig(WithClusterId()));
            serializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            await serializer.SerializeAsync(1, ValueContext(testTopic));

            Assert.Equal(0, calls);
            Assert.True(subjectStore.ContainsKey("configured-subject"));
            Assert.False(subjectStore.ContainsKey("resolved-subject"));
        }

        [Fact]
        public async Task Serde_IgnoresTheResolver_WhenStrategyIsNotAssociated()
        {
            Associate(ClusterId, "resolved-subject");

            int calls = 0;
            var serializer = new AvroSerializer<int>(
                schemaRegistryClient,
                new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic });
            serializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            await serializer.SerializeAsync(1, ValueContext(testTopic));

            Assert.Equal(0, calls);
            Assert.True(subjectStore.ContainsKey($"{testTopic}-value"));
        }

        [Fact]
        public async Task Serde_TheLatestResolver_IsRetained()
        {
            Associate("lkc-first", "first-subject");
            Associate("lkc-second", "second-subject");

            var serializer = new AvroSerializer<int>(schemaRegistryClient);
            serializer.SetClusterIdResolver(() => "lkc-first");
            serializer.SetClusterIdResolver(() => "lkc-second");

            await serializer.SerializeAsync(1, ValueContext(testTopic));

            Assert.False(subjectStore.ContainsKey("first-subject"));
            Assert.True(subjectStore.ContainsKey("second-subject"));
        }

        // The extension methods must see through the sync-over-async adapter, since
        // that is how an async serde reaches a producer or consumer.

        [Fact]
        public void Extensions_SeeThroughTheSyncOverAsyncSerializerAdapter()
        {
            Associate(ClusterId, "resolved-subject");

            ISerializer<int> serializer =
                new AvroSerializer<int>(schemaRegistryClient).AsSyncOverAsync();
            serializer.SetClusterIdResolver(() => ClusterId);

            serializer.Serialize(1, ValueContext(testTopic));

            Assert.True(subjectStore.ContainsKey("resolved-subject"));
        }

        [Fact]
        public void Extensions_SeeThroughTheSyncOverAsyncDeserializerAdapter()
        {
            Associate(ClusterId, "resolved-subject");

            var bytes = new AvroSerializer<int>(schemaRegistryClient,
                new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic })
                .AsSyncOverAsync()
                .Serialize(1, ValueContext(testTopic));

            int calls = 0;
            IDeserializer<int> deserializer =
                new AvroDeserializer<int>(schemaRegistryClient).AsSyncOverAsync();
            deserializer.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            Assert.Equal(1, deserializer.Deserialize(bytes, false, ValueContext(testTopic)));
            Assert.Equal(1, calls);
        }

        // A serde with no cluster id concept must tolerate being handed a resolver
        // anyway.

        [Fact]
        public void Extensions_SetClusterIdResolverIsANoOpForABuiltInSerializer()
        {
            // Must not throw.
            Serializers.Utf8.SetClusterIdResolver(() => ClusterId);
            Deserializers.Utf8.SetClusterIdResolver(() => ClusterId);
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

        // The resolver is what a producer or consumer would otherwise have had to
        // block on during construction, so the strategy must not invoke it until
        // a lookup actually needs the cluster id, and then only once.

        [Fact]
        public async Task Resolver_IsNotInvokedUntilTheFirstLookup()
        {
            int calls = 0;
            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);
            strategy.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            Assert.Equal(0, calls);

            await strategy.GetSubjectNameAsync(ValueContext(testTopic), null);

            Assert.Equal(1, calls);
        }

        [Fact]
        public async Task Resolver_IsInvokedPerLookup_AndNotForCachedSubjects()
        {
            // The strategy does not cache the cluster id itself: the subject name
            // cache already makes lookups rare, and the client caches the id.
            int calls = 0;
            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);
            strategy.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            await strategy.GetSubjectNameAsync(ValueContext(testTopic), null);
            await strategy.GetSubjectNameAsync(KeyContext(testTopic), null);
            await strategy.GetSubjectNameAsync(ValueContext("another-topic"), null);
            Assert.Equal(3, calls);

            await strategy.GetSubjectNameAsync(ValueContext(testTopic), null);
            await strategy.GetSubjectNameAsync(KeyContext(testTopic), null);
            await strategy.GetSubjectNameAsync(ValueContext("another-topic"), null);
            Assert.Equal(3, calls);
        }

        [Fact]
        public async Task ResolvedClusterId_IsTheLookupNamespace()
        {
            Associate(ClusterId, "resolved-subject");
            Associate(AssociatedNameStrategy.NamespaceWildcard, "wildcard-subject");

            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);
            strategy.SetClusterIdResolver(() => ClusterId);

            Assert.Equal("resolved-subject",
                await strategy.GetSubjectNameAsync(ValueContext(testTopic), null));
        }

        [Fact]
        public async Task WithoutAResolver_TheWildcardNamespaceIsUsed()
        {
            Associate(AssociatedNameStrategy.NamespaceWildcard, "wildcard-subject");

            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);

            Assert.Equal("wildcard-subject",
                await strategy.GetSubjectNameAsync(ValueContext(testTopic), null));
        }

        [Fact]
        public async Task AConfiguredClusterId_WinsOverTheResolver()
        {
            Associate(ConfiguredClusterId, "configured-subject");
            Associate(ClusterId, "resolved-subject");

            int calls = 0;
            var strategy = new AssociatedNameStrategy(schemaRegistryClient, WithClusterId());
            strategy.SetClusterIdResolver(() => { ++calls; return ClusterId; });

            Assert.Equal("configured-subject",
                await strategy.GetSubjectNameAsync(ValueContext(testTopic), null));
            Assert.Equal(0, calls);
        }

        [Fact]
        public async Task TheLatestResolver_IsRetained()
        {
            Associate("lkc-first", "first-subject");
            Associate("lkc-second", "second-subject");

            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);
            strategy.SetClusterIdResolver(() => "lkc-first");
            strategy.SetClusterIdResolver(() => "lkc-second");

            Assert.Equal("second-subject",
                await strategy.GetSubjectNameAsync(ValueContext(testTopic), null));
        }

        [Fact]
        public async Task AnUnresolvableClusterId_Throws_AndIsRetriedNextTime()
        {
            Associate(ClusterId, "resolved-subject");

            string resolved = null;
            int calls = 0;
            var strategy = new AssociatedNameStrategy(schemaRegistryClient, null);
            strategy.SetClusterIdResolver(() => { ++calls; return resolved; });

            // A resolver returns null when the client could not reach a broker in
            // time. That must not silently degrade to the wildcard namespace.
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => strategy.GetSubjectNameAsync(ValueContext(testTopic), null));

            resolved = ClusterId;

            Assert.Equal("resolved-subject",
                await strategy.GetSubjectNameAsync(ValueContext(testTopic), null));
            Assert.Equal(2, calls);
        }

        private static SerializationContext ValueContext(string topic)
            => new SerializationContext(MessageComponentType.Value, topic);

        private static SerializationContext KeyContext(string topic)
            => new SerializationContext(MessageComponentType.Key, topic);

        private void Associate(string resourceNamespace, string subject)
            => associationStore[$"{resourceNamespace}/{testTopic}"] = new List<Association>
            {
                new Association(
                    subject: subject,
                    guid: Guid.NewGuid().ToString(),
                    resourceName: testTopic,
                    resourceNamespace: resourceNamespace,
                    resourceId: "resource-id",
                    resourceType: "topic",
                    associationType: "value",
                    lifecycle: "WEAK",
                    frozen: false)
            };
    }

    /// <summary>
    ///     A minimal record for the JSON serdes, which require a reference type.
    /// </summary>
    public class UnitTestJsonRecord
    {
        public int Value { get; set; }
    }
}
