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
    ///     Tests that a serializer and a serializer builder cannot both be
    ///     configured for the same message component, and that neither may be
    ///     specified more than once.
    /// </summary>
    public class SerdeBuilderConfigurationTests
    {
        private static ProducerConfig ProducerConfig()
            => new ProducerConfig { BootstrapServers = "localhost:9092" };

        private static ConsumerConfig ConsumerConfig()
            => new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" };

        // Producer: a serializer and a serializer builder are mutually exclusive.

        [Fact]
        public void Producer_RejectsKeySerializerThenBuilder()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializer(Serializers.Utf8);

            Assert.Throws<InvalidOperationException>(
                () => builder.SetKeySerializerBuilder(new StubSerializerBuilder<string>()));
        }

        [Fact]
        public void Producer_RejectsKeyBuilderThenSerializer()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializerBuilder(new StubSerializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetKeySerializer(Serializers.Utf8));
        }

        [Fact]
        public void Producer_RejectsValueSerializerThenBuilder()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializer(Serializers.Utf8);

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueSerializerBuilder(new StubSerializerBuilder<string>()));
        }

        [Fact]
        public void Producer_RejectsValueBuilderThenSerializer()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializerBuilder(new StubSerializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueSerializer(Serializers.Utf8));
        }

        [Fact]
        public void Producer_RejectsTheSameBuilderTwice()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializerBuilder(new StubSerializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueSerializerBuilder(new StubSerializerBuilder<string>()));
        }

        [Fact]
        public void Producer_RejectsSyncAndAsyncBuildersTogether()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializerBuilder(new StubSerializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueSerializerBuilder(new StubAsyncSerializerBuilder<string>()));
        }

        [Fact]
        public void Producer_RejectsAsyncSerializerThenBuilder()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializer(new StubAsyncSerializer<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueSerializerBuilder(new StubAsyncSerializerBuilder<string>()));
        }

        // Consumer: the same, for deserializers.

        [Fact]
        public void Consumer_RejectsKeyDeserializerThenBuilder()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetKeyDeserializer(Deserializers.Utf8);

            Assert.Throws<InvalidOperationException>(
                () => builder.SetKeyDeserializerBuilder(new StubDeserializerBuilder<string>()));
        }

        [Fact]
        public void Consumer_RejectsKeyBuilderThenDeserializer()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetKeyDeserializerBuilder(new StubDeserializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetKeyDeserializer(Deserializers.Utf8));
        }

        [Fact]
        public void Consumer_RejectsValueDeserializerThenBuilder()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetValueDeserializer(Deserializers.Utf8);

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueDeserializerBuilder(new StubDeserializerBuilder<string>()));
        }

        [Fact]
        public void Consumer_RejectsValueBuilderThenDeserializer()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetValueDeserializerBuilder(new StubDeserializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueDeserializer(Deserializers.Utf8));
        }

        [Fact]
        public void Consumer_RejectsSyncAndAsyncBuildersTogether()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig())
                .SetValueDeserializerBuilder(new StubDeserializerBuilder<string>());

            Assert.Throws<InvalidOperationException>(
                () => builder.SetValueDeserializerBuilder(new StubAsyncDeserializerBuilder<string>()));
        }

        // Dependent producer: the same exclusion, and the builder receives the
        // configuration of the producer owning the handle.

        [Fact]
        public void DependentProducer_RejectsSerializerThenBuilder()
        {
            using (var parent = new ProducerBuilder<Null, Null>(ProducerConfig()).Build())
            {
                var builder = new DependentProducerBuilder<string, string>(parent.Handle)
                    .SetValueSerializer(Serializers.Utf8);

                Assert.Throws<InvalidOperationException>(
                    () => builder.SetValueSerializerBuilder(new StubSerializerBuilder<string>()));
            }
        }

        [Fact]
        public void DependentProducer_RejectsBuilderThenSerializer()
        {
            using (var parent = new ProducerBuilder<Null, Null>(ProducerConfig()).Build())
            {
                var builder = new DependentProducerBuilder<string, string>(parent.Handle)
                    .SetKeySerializerBuilder(new StubAsyncSerializerBuilder<string>());

                Assert.Throws<InvalidOperationException>(
                    () => builder.SetKeySerializer(Serializers.Utf8));
            }
        }

        [Fact]
        public void DependentProducer_RejectsSyncAndAsyncBuildersTogether()
        {
            using (var parent = new ProducerBuilder<Null, Null>(ProducerConfig()).Build())
            {
                var builder = new DependentProducerBuilder<string, string>(parent.Handle)
                    .SetValueSerializerBuilder(new StubSerializerBuilder<string>());

                Assert.Throws<InvalidOperationException>(
                    () => builder.SetValueSerializerBuilder(new StubAsyncSerializerBuilder<string>()));
            }
        }

        [Fact]
        public void DependentProducer_BuilderReceivesTheParentConfigAndIsKeyFlag()
        {
            var keyBuilder = new RecordingSerializerBuilder();
            var valueBuilder = new RecordingSerializerBuilder();

            using (var parent = new ProducerBuilder<Null, Null>(ProducerConfig()).Build())
            using (new DependentProducerBuilder<string, string>(parent.Handle)
                .SetKeySerializerBuilder(keyBuilder)
                .SetValueSerializerBuilder(valueBuilder)
                .Build())
            {
            }

            Assert.True(keyBuilder.IsKey);
            Assert.False(valueBuilder.IsKey);
            Assert.Contains(keyBuilder.Config,
                kvp => kvp.Key == "bootstrap.servers" && kvp.Value == "localhost:9092");
            Assert.Contains(valueBuilder.Config,
                kvp => kvp.Key == "bootstrap.servers" && kvp.Value == "localhost:9092");
        }

        // Setting only a builder is accepted.

        [Fact]
        public void Producer_AcceptsABuilderAlone()
        {
            var builder = new ProducerBuilder<string, string>(ProducerConfig());

            var chained = builder
                .SetKeySerializerBuilder(new StubSerializerBuilder<string>())
                .SetValueSerializerBuilder(new StubAsyncSerializerBuilder<string>());

            Assert.Same(builder, chained);
        }

        [Fact]
        public void Consumer_AcceptsABuilderAlone()
        {
            var builder = new ConsumerBuilder<string, string>(ConsumerConfig());

            var chained = builder
                .SetKeyDeserializerBuilder(new StubDeserializerBuilder<string>())
                .SetValueDeserializerBuilder(new StubAsyncDeserializerBuilder<string>());

            Assert.Same(builder, chained);
        }

        // The builder receives the client's configuration and the key/value flag.

        [Fact]
        public void Builder_ReceivesTheClientConfigAndIsKeyFlag()
        {
            var keyBuilder = new RecordingSerializerBuilder();
            var valueBuilder = new RecordingSerializerBuilder();

            using (new ProducerBuilder<string, string>(ProducerConfig())
                .SetKeySerializerBuilder(keyBuilder)
                .SetValueSerializerBuilder(valueBuilder)
                .Build())
            {
            }

            Assert.True(keyBuilder.Built);
            Assert.True(keyBuilder.IsKey);
            Assert.True(valueBuilder.Built);
            Assert.False(valueBuilder.IsKey);

            Assert.Contains(keyBuilder.Config,
                kvp => kvp.Key == "bootstrap.servers" && kvp.Value == "localhost:9092");
        }

        [Fact]
        public void Build_DoesNotWaitOnTheBroker()
        {
            // No broker is reachable here, so construction must not ask for the
            // cluster id itself - that is deferred to the serializer's first use.
            var valueBuilder = new RecordingSerializerBuilder();

            using (new ProducerBuilder<string, string>(ProducerConfig())
                .SetValueSerializerBuilder(valueBuilder)
                .Build())
            {
            }

            Assert.True(valueBuilder.Built);
        }

        private class StubSerializerBuilder<T> : ISerializerBuilder<T>
        {
            public ISerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => new StubSerializer<T>();
        }

        private class StubAsyncSerializerBuilder<T> : IAsyncSerializerBuilder<T>
        {
            public IAsyncSerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => new StubAsyncSerializer<T>();
        }

        private class StubDeserializerBuilder<T> : IDeserializerBuilder<T>
        {
            public IDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => new StubDeserializer<T>();
        }

        private class StubAsyncDeserializerBuilder<T> : IAsyncDeserializerBuilder<T>
        {
            public IAsyncDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
                => new StubAsyncDeserializer<T>();
        }

        private class RecordingSerializerBuilder : ISerializerBuilder<string>
        {
            public bool Built { get; private set; }
            public bool IsKey { get; private set; }
            public IEnumerable<KeyValuePair<string, string>> Config { get; private set; }

            public ISerializer<string> Build(
                IEnumerable<KeyValuePair<string, string>> config, bool isKey)
            {
                Built = true;
                IsKey = isKey;
                Config = config;
                return new StubSerializer<string>();
            }
        }

        private class StubSerializer<T> : ISerializer<T>
        {
            public byte[] Serialize(T data, SerializationContext context)
                => new byte[0];
        }

        private class StubAsyncSerializer<T> : IAsyncSerializer<T>
        {
            public Task<byte[]> SerializeAsync(T data, SerializationContext context)
                => Task.FromResult(new byte[0]);
        }

        private class StubDeserializer<T> : IDeserializer<T>
        {
            public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                => default;
        }

        private class StubAsyncDeserializer<T> : IAsyncDeserializer<T>
        {
            public Task<T> DeserializeAsync(
                ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
                => Task.FromResult(default(T));
        }
    }
}
