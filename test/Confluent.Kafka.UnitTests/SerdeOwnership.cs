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
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    /// <summary>
    ///     Tests that a producer or consumer releases the serializers and
    ///     deserializers it constructed from a builder - when it is disposed, and
    ///     when its own construction fails - and leaves application-supplied ones
    ///     alone.
    /// </summary>
    public class SerdeOwnershipTests
    {
        private static ProducerConfig ProducerConfig()
            => new ProducerConfig { BootstrapServers = "localhost:9092" };

        private static ConsumerConfig ConsumerConfig()
            => new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" };

        // Disposal along with the client.

        [Fact]
        public void Producer_DisposesBuilderBuiltSerializers()
        {
            var key = new TrackingSerializer();
            var value = new TrackingSerializer();

            using (new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializerBuilder(new StubBuilder(key))
                .SetValueSerializerBuilder(new StubBuilder(value))
                .Build())
            {
                Assert.False(key.Disposed);
                Assert.False(value.Disposed);
            }

            Assert.True(key.Disposed);
            Assert.True(value.Disposed);
        }

        [Fact]
        public void Producer_DisposesBuilderBuiltAsyncSerializers()
        {
            var key = new TrackingAsyncSerializer();
            var value = new TrackingAsyncSerializer();

            using (new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializerBuilder(new StubAsyncBuilder(key))
                .SetValueSerializerBuilder(new StubAsyncBuilder(value))
                .Build())
            {
            }

            Assert.True(key.Disposed);
            Assert.True(value.Disposed);
        }

        [Fact]
        public void Producer_LeavesApplicationSuppliedSerializersAlone()
        {
            var key = new TrackingSerializer();
            var value = new TrackingAsyncSerializer();

            using (new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializer(key)
                .SetValueSerializer(value)
                .Build())
            {
            }

            Assert.False(key.Disposed);
            Assert.False(value.Disposed);
        }

        [Fact]
        public void Consumer_DisposesBuilderBuiltDeserializers()
        {
            var key = new TrackingDeserializer();
            var value = new TrackingAsyncDeserializer();

            using (new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetKeyDeserializerBuilder(new StubBuilder(key))
                .SetValueDeserializerBuilder(new StubAsyncBuilder(value))
                .Build())
            {
                Assert.False(key.Disposed);
                Assert.False(value.Disposed);
            }

            Assert.True(key.Disposed);
            Assert.True(value.Disposed);
        }

        [Fact]
        public void Consumer_LeavesApplicationSuppliedDeserializersAlone()
        {
            var key = new TrackingDeserializer();

            using (new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetKeyDeserializer(key)
                .Build())
            {
            }

            Assert.False(key.Disposed);
        }

        // Disposal when construction fails: a constructor that throws never
        // reaches Dispose, so whatever was already built must be released on the
        // way out.

        [Fact]
        public void Producer_ReleasesABuiltSerializer_WhenTheOtherBuilderThrows()
        {
            var key = new TrackingAsyncSerializer();

            Assert.Throws<InvalidOperationException>(() =>
                new ProducerBuilder<string, string>(ProducerConfig())
                    .SetKeySerializerBuilder(new StubAsyncBuilder(key))
                    .SetValueSerializerBuilder(new ThrowingBuilder())
                    .Build());

            Assert.True(key.Disposed);
        }

        [Fact]
        public void Producer_ReleasesABuiltSerializer_WhenTheOtherHasNoDefault()
        {
            var key = new TrackingSerializer();

            Assert.Throws<ArgumentNullException>(() =>
                new ProducerBuilder<string, NoDefaultSerde>(ProducerConfig())
                    .SetKeySerializerBuilder(new StubBuilder(key))
                    .Build());

            Assert.True(key.Disposed);
        }

        [Fact]
        public void Consumer_ReleasesABuiltDeserializer_WhenTheOtherBuilderThrows()
        {
            var key = new TrackingDeserializer();

            Assert.Throws<InvalidOperationException>(() =>
                new ConsumerBuilder<string, string>(ConsumerConfig())
                    .SetKeyDeserializerBuilder(new StubBuilder(key))
                    .SetValueDeserializerBuilder(new ThrowingBuilder())
                    .Build());

            Assert.True(key.Disposed);
        }

        [Fact]
        public void Consumer_ReleasesABuiltDeserializer_WhenTheOtherHasNoDefault()
        {
            var key = new TrackingAsyncDeserializer();

            Assert.Throws<InvalidOperationException>(() =>
                new ConsumerBuilder<string, NoDefaultSerde>(ConsumerConfig())
                    .SetKeyDeserializerBuilder(new StubAsyncBuilder(key))
                    .Build());

            Assert.True(key.Disposed);
        }

        // Failures after the native handle exists must release it too; a serde
        // rejecting the cluster id resolver is the one such failure that
        // application code can cause.

        [Fact]
        public void Producer_ReleasesEverything_WhenASerdeRejectsTheClusterIdResolver()
        {
            var key = new TrackingSerializer();
            var value = new ResolverRejectingSerializer();
            PartitionerDelegate partitioner = (topic, count, keyData, keyIsNull) => 0;

            // The partitioner exercises the release of its pinned delegate, which
            // must happen only after the native handle has been destroyed.
            Assert.Throws<NotSupportedException>(() =>
                new ProducerBuilder<string, string>(ProducerConfig())
                    .SetDefaultPartitioner(partitioner)
                    .SetKeySerializerBuilder(new StubBuilder(key))
                    .SetValueSerializerBuilder(new StubBuilder(value))
                    .Build());

            Assert.True(key.Disposed);
            Assert.True(value.Disposed);
        }

        [Fact]
        public void Consumer_ReleasesEverything_WhenASerdeRejectsTheClusterIdResolver()
        {
            var key = new TrackingDeserializer();
            var value = new ResolverRejectingDeserializer();

            Assert.Throws<NotSupportedException>(() =>
                new ConsumerBuilder<string, string>(ConsumerConfig())
                    .SetKeyDeserializerBuilder(new StubBuilder(key))
                    .SetValueDeserializerBuilder(new StubBuilder(value))
                    .Build());

            Assert.True(key.Disposed);
            Assert.True(value.Disposed);
        }

        private class NoDefaultSerde
        {
        }

        private class ResolverRejectingSerializer : TrackingSerializer, IClusterIdAware
        {
            public void SetClusterIdResolver(Func<string> clusterIdResolver)
                => throw new NotSupportedException("no cluster id here");
        }

        private class ResolverRejectingDeserializer : TrackingDeserializer, IClusterIdAware
        {
            public void SetClusterIdResolver(Func<string> clusterIdResolver)
                => throw new NotSupportedException("no cluster id here");
        }

        private class TrackingSerializer : ISerializer<string>, ISerdeOwnedResources
        {
            public bool Disposed { get; private set; }

            public void DisposeOwnedResources()
                => Disposed = true;

            public byte[] Serialize(string data, SerializationContext context)
                => Serializers.Utf8.Serialize(data, context);
        }

        private class TrackingAsyncSerializer : IAsyncSerializer<string>, ISerdeOwnedResources
        {
            public bool Disposed { get; private set; }

            public void DisposeOwnedResources()
                => Disposed = true;

            public Task<byte[]> SerializeAsync(string data, SerializationContext context)
                => Task.FromResult(Serializers.Utf8.Serialize(data, context));
        }

        private class TrackingDeserializer : IDeserializer<string>, ISerdeOwnedResources
        {
            public bool Disposed { get; private set; }

            public void DisposeOwnedResources()
                => Disposed = true;

            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                => Deserializers.Utf8.Deserialize(data, isNull, context);
        }

        private class TrackingAsyncDeserializer : IAsyncDeserializer<string>, ISerdeOwnedResources
        {
            public bool Disposed { get; private set; }

            public void DisposeOwnedResources()
                => Disposed = true;

            public Task<string> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
                => Task.FromResult(Deserializers.Utf8.Deserialize(data.Span, isNull, context));
        }

        private class StubBuilder : ISerializerBuilder<string>, IDeserializerBuilder<string>
        {
            private readonly ISerializer<string> serializer;
            private readonly IDeserializer<string> deserializer;

            public StubBuilder(ISerializer<string> serializer) => this.serializer = serializer;
            public StubBuilder(IDeserializer<string> deserializer) => this.deserializer = deserializer;

            ISerializer<string> ISerializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey) => serializer;

            IDeserializer<string> IDeserializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey) => deserializer;
        }

        private class StubAsyncBuilder : IAsyncSerializerBuilder<string>, IAsyncDeserializerBuilder<string>
        {
            private readonly IAsyncSerializer<string> serializer;
            private readonly IAsyncDeserializer<string> deserializer;

            public StubAsyncBuilder(IAsyncSerializer<string> serializer) => this.serializer = serializer;
            public StubAsyncBuilder(IAsyncDeserializer<string> deserializer) => this.deserializer = deserializer;

            IAsyncSerializer<string> IAsyncSerializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey) => serializer;

            IAsyncDeserializer<string> IAsyncDeserializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey) => deserializer;
        }

        private class ThrowingBuilder : ISerializerBuilder<string>, IDeserializerBuilder<string>
        {
            ISerializer<string> ISerializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => throw new InvalidOperationException("builder failed");

            IDeserializer<string> IDeserializerBuilder<string>.Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => throw new InvalidOperationException("builder failed");
        }
    }
}
