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
using Confluent.SchemaRegistry.Serdes;


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
            var config = new Dictionary<string, string>
            {
                { "avro.serializer.buffer.bytes", "42" },
                { "avro.serializer.auto.register.schemas", "false" }
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

            Assert.Throws<ArgumentException>(() => { var avroSerializer = new AvroSerializer<int>(null, config); });
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

            Assert.Throws<ArgumentException>(() => { var avroSerializer = new AvroSerializer<int>(null, config); });
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
