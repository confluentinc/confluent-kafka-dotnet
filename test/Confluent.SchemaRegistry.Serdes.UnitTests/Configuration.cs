// Copyright 2018-2020 Confluent Inc.
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


namespace Confluent.SchemaRegistry.Serdes.UnitTests
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
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync(testTopic + "-value", It.IsAny<Schema>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) => new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Avro)
            );
            schemaRegistryClient = schemaRegistryMock.Object;
        }

        [Fact]
        public void SerializerConfigure()
        {
            var config = new AvroSerializerConfig
            {
                BufferBytes = 42,
                AutoRegisterSchemas = false
            };

            // should not throw.
            var avroSerializer = new AvroSerializer<int>(null, config);
        }

        [Fact]
        public void DeserializerConfigure()
        {
            var config = new Dictionary<string, string> { };

            // should not throw.
            var avroDeserializer = new AvroDeserializer<int>(null, config);
        }

        [Fact]
        public void DeserializerThrowsOnUnrelated()
        {
            var config = new Dictionary<string, string>
            {
                { "some.random.config.param", "false" }
            };

            Assert.Throws<ArgumentException>(() => { var avroDeserializer = new AvroDeserializer<int>(null, config); });
        }

        [Fact]
        public void SerializerThrowsOnUnrelated()
        {
            var config = new Dictionary<string, string>
            {
                { "some.random.config.param", "false" }
            };
            var config_ = new AvroSerializerConfig(config);

            Assert.Throws<ArgumentException>(() => { var avroSerializer = new AvroSerializer<int>(null, config_); });
        }

        [Fact]
        public void SerializerUnexpectedAvroConfigParam()
        {
            var config = new Dictionary<string, string>
            {
                { "avro.serializer.buffer.bytes", "42" },
                { "avro.serializer.auto.register.schemas", "false" },
                { "avro.unknown", "70" }
            };
            var config_ = new AvroSerializerConfig(config);

            Assert.Throws<ArgumentException>(() => { var avroSerializer = new AvroSerializer<int>(null, config_); });
        }

        [Fact]
        public void DeserializerUnexpectedAvroConfigParam()
        {
            var config = new Dictionary<string, string>
            {
                { "avro.serializer.auto.register.schemas", "false" }
            };

            Assert.Throws<ArgumentException>(() => {  var avroDeserializer = new AvroDeserializer<int>(null, config); });
        }
    }
}
