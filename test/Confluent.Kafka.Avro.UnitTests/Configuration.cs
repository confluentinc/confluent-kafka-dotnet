// Copyright 2018 Confluent Inc.
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

using Moq;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.SchemaRegistry;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Avro.UnitTests
{
    public class ConfigurationTests
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public ConfigurationTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic)).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>())).ReturnsAsync(
                (string topic, string schema) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>())).ReturnsAsync((int id) => store.Where(x => x.Value == id).First().Key);
            schemaRegistryClient = schemaRegistryMock.Object;
        }

        [Fact]
        public void SerializerConfigure()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "avro.serializer.buffer.bytes", 42 },
                { "avro.serializer.auto.register.schemas", false }
            };

            var modifiedConfig = avroSerializer.Configure(config, true);

            Assert.Equal(0, modifiedConfig.Count());
        }

        [Fact]
        public void DeserializerConfigure()
        {
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
            };

            var modifiedConfig = avroDeserializer.Configure(config, true);

            Assert.Equal(0, modifiedConfig.Count());
        }

        [Fact]
        public void DeserializerIgnoresUnrelated()
        {
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "some.random.config.param", false }
            };

            var modifiedConfig = avroDeserializer.Configure(config, true);

            Assert.Equal(1, modifiedConfig.Count());
        }

        [Fact]
        public void SerializerIgnoresUnrelated()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "some.random.config.param", false }
            };

            var modifiedConfig = avroSerializer.Configure(config, true);

            Assert.Equal(1, modifiedConfig.Count());
        }

        [Fact]
        public void SerializerUnexpectedConfigParam()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "avro.serializer.buffer.bytes", 42 },
                { "avro.serializer.auto.register.schemas", false },
                { "avro.unknown", 70 }
            };

            Assert.Throws<ArgumentException>(() => { avroSerializer.Configure(config, true); });
        }

        [Fact]
        public void DeserializerUnexpectedConfigParam()
        {
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "avro.serializer.auto.register.schemas", false }
            };

            Assert.Throws<ArgumentException>(() => { avroDeserializer.Configure(config, true); });
        }

        [Fact]
        public void SerializerDoubleConfigureSchemaRegistry()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "localhost:8081" }
            };

            Assert.Throws<ArgumentException>(() => { avroSerializer.Configure(config, true); });
        }

        [Fact]
        public void DeserializerDoubleConfigureSchemaRegistry()
        {
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "localhost:8081" }
            };

            Assert.Throws<ArgumentException>(() => { avroDeserializer.Configure(config, true); });
        }
    }
}
