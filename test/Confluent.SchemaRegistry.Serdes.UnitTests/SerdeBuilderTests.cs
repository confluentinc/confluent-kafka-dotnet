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

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using Confluent.Kafka;
using Moq;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests for the Schema Registry serde builders: how the Schema Registry
    ///     client is resolved, which combinations of settings are rejected, and who
    ///     owns the resulting client.
    /// </summary>
    public class SerdeBuilderTests : BaseSerializeDeserializeTests
    {
        private static SchemaRegistryConfig Config()
            => new SchemaRegistryConfig { Url = "http://localhost:8081" };

        private static readonly IEnumerable<KeyValuePair<string, string>> ClientConfig =
            new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("bootstrap.servers", "localhost:9092")
            };

        [Fact]
        public void Build_FromAnInjectedClient()
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .Build(ClientConfig, false);

            Assert.NotNull(serializer);
        }

        [Fact]
        public void Build_FromConfiguration()
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .Build(ClientConfig, false);

            Assert.NotNull(serializer);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void Build_ForBothKeyAndValue(bool isKey)
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .Build(ClientConfig, isKey);

            var deserializer = new AvroDeserializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .Build(ClientConfig, isKey);

            Assert.NotNull(serializer);
            Assert.NotNull(deserializer);
        }

        [Fact]
        public void Build_AppliesTheSerializerConfig()
        {
            // The Topic strategy does not use the cluster id, so a serializer built
            // with it reports no need - which shows the config reached the serde.
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .SetSerializerConfig(new AvroSerializerConfig
                {
                    SubjectNameStrategy = SubjectNameStrategy.Topic
                })
                .Build(ClientConfig, false);

            Assert.False(serializer.NeedsClusterId());
        }

        [Fact]
        public void Build_TheResultingSerdeNeedsTheClusterIdByDefault()
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .Build(ClientConfig, false);

            Assert.True(serializer.NeedsClusterId());
        }

        [Fact]
        public void Setters_Chain()
        {
            var builder = new AvroSerializerBuilder<int>();

            var chained = builder
                .SetSchemaRegistryConfig(Config())
                .SetWebProxy(new WebProxy("http://localhost:3128"))
                .SetRuleRegistry(new RuleRegistry())
                .SetSerializerConfig(new AvroSerializerConfig());

            Assert.Same(builder, chained);
        }

        // Rejected combinations.

        [Fact]
        public void Reject_BothClientAndConfig()
        {
            var builder = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .SetSchemaRegistryConfig(Config());

            var ex = Assert.Throws<ArgumentException>(() => builder.Build(ClientConfig, false));
            Assert.Contains("one or the other", ex.Message);
        }

        [Fact]
        public void Reject_NeitherClientNorConfig()
        {
            var builder = new AvroSerializerBuilder<int>();

            var ex = Assert.Throws<ArgumentException>(() => builder.Build(ClientConfig, false));
            Assert.Contains("must be specified", ex.Message);
        }

        [Fact]
        public void Reject_AuthenticationProviderAlongsideAClient()
        {
            var builder = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .SetAuthenticationHeaderValueProvider(new StubAuthenticationHeaderValueProvider());

            var ex = Assert.Throws<ArgumentException>(() => builder.Build(ClientConfig, false));
            Assert.Contains("authentication header value provider", ex.Message);
        }

        [Fact]
        public void Reject_ProxyAlongsideAClient()
        {
            var builder = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .SetWebProxy(new WebProxy("http://localhost:3128"));

            var ex = Assert.Throws<ArgumentException>(() => builder.Build(ClientConfig, false));
            Assert.Contains("proxy", ex.Message);
        }

        [Fact]
        public void Reject_DeserializerWithBothClientAndConfig()
        {
            var builder = new AvroDeserializerBuilder<int>()
                .SetSchemaRegistryClient(schemaRegistryClient)
                .SetSchemaRegistryConfig(Config());

            Assert.Throws<ArgumentException>(() => builder.Build(ClientConfig, false));
        }

        // Client construction from configuration.

        [Fact]
        public void Build_UsesTheSuppliedAuthenticationProvider()
        {
            var provider = new StubAuthenticationHeaderValueProvider();

            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .SetAuthenticationHeaderValueProvider(provider)
                .Build(ClientConfig, false);

            Assert.Same(provider, ResolvedClient(serializer).AuthHeaderProvider);
        }

        [Fact]
        public void Build_UsesTheSuppliedProxy()
        {
            var proxy = new WebProxy("http://localhost:3128");

            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .SetWebProxy(proxy)
                .Build(ClientConfig, false);

            Assert.Same(proxy, ResolvedClient(serializer).Proxy);
        }

        [Fact]
        public void Build_AProviderIsReReadPerRequest()
        {
            // Credentials can only change inside a provider instance, since the
            // client's provider is fixed once constructed. Confirm the provider is
            // consulted on each call rather than cached.
            var provider = new StubAuthenticationHeaderValueProvider();

            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .SetAuthenticationHeaderValueProvider(provider)
                .Build(ClientConfig, false);

            var resolved = ResolvedClient(serializer).AuthHeaderProvider;

            var first = resolved.GetAuthenticationHeader();
            var second = resolved.GetAuthenticationHeader();

            Assert.Equal("token-1", first.Parameter);
            Assert.Equal("token-2", second.Parameter);
        }

        // Ownership of the schema registry client.

        [Fact]
        public void Ownership_AnInjectedClientIsNotOwned()
        {
            var clientMock = new Mock<ISchemaRegistryClient>();

            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryClient(clientMock.Object)
                .Build(ClientConfig, false);

            serializer.Dispose();

            clientMock.Verify(x => x.Dispose(), Times.Never());
        }

        [Fact]
        public void Ownership_AnInjectedClientSurvivesRepeatedDisposal()
        {
            var clientMock = new Mock<ISchemaRegistryClient>();

            var deserializer = new AvroDeserializerBuilder<int>()
                .SetSchemaRegistryClient(clientMock.Object)
                .Build(ClientConfig, false);

            deserializer.Dispose();
            deserializer.Dispose();

            clientMock.Verify(x => x.Dispose(), Times.Never());
        }

        [Fact]
        public void Ownership_AConfiguredClientIsOwnedAndReleased()
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .Build(ClientConfig, false);

            Assert.True(Owns(serializer));

            serializer.Dispose();

            Assert.False(Owns(serializer));
        }

        [Fact]
        public void Ownership_AConfiguredClientIsOwnedAndReleased_Deserializer()
        {
            var deserializer = new AvroDeserializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .Build(ClientConfig, false);

            Assert.True(Owns(deserializer));

            deserializer.Dispose();

            Assert.False(Owns(deserializer));
        }

        [Fact]
        public void Ownership_DisposingTwiceIsANoOp()
        {
            var serializer = new AvroSerializerBuilder<int>()
                .SetSchemaRegistryConfig(Config())
                .Build(ClientConfig, false);

            serializer.Dispose();
            serializer.Dispose();

            Assert.False(Owns(serializer));
        }

        [Fact]
        public void Ownership_ASerdeConstructedDirectlyOwnsNothing()
        {
            var clientMock = new Mock<ISchemaRegistryClient>();
            var serializer = new AvroSerializer<int>(clientMock.Object);

            serializer.Dispose();

            clientMock.Verify(x => x.Dispose(), Times.Never());
        }

        private static bool Owns(object serde)
            => (bool)serde.GetType()
                .GetField("ownsSchemaRegistryClient",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(serde);

        private static ISchemaRegistryClient ResolvedClient(object serde)
            => (ISchemaRegistryClient)serde.GetType()
                .GetField("schemaRegistryClient",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(serde);

        /// <summary>
        ///     A provider that returns a different value on each call, standing in
        ///     for credentials that rotate over the lifetime of a client.
        /// </summary>
        private class StubAuthenticationHeaderValueProvider : IAuthenticationHeaderValueProvider
        {
            private int calls;

            public AuthenticationHeaderValue GetAuthenticationHeader()
                => new AuthenticationHeaderValue("Bearer", $"token-{++calls}");
        }
    }
}
