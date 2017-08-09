// Copyright 2016-2017 Confluent Inc.
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
using Avro;
using Avro.Generic;
using Confluent.Kafka.Serialization;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Confluent.Kafka.SchemaRegistry.UnitTests.Serializer
{
    public class AvroSerializerTests
    {
        private GenericRecord CreateAvroRecord()
        {
            string userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                                "\"name\": \"User\"," +
                                "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
            var schema = Avro.Schema.Parse(userSchema) as RecordSchema;
            var avroRecord = new GenericRecord(schema);
            avroRecord.Add("name", "testUser");
            return avroRecord;
        }

        private ISchemaRegistryClient schemaRegistry;
        private string topic;

        public AvroSerializerTests()
        {
            var dico = new Dictionary<string, int>();
            topic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.GetRegistrySubject(topic, false)).Returns("topic-value");
            schemaRegistryMock.Setup(x => x.RegisterAsync("topic-value", It.IsAny<string>())).ReturnsAsync((string top, string schem) => dico.TryGetValue(schem, out int id) ? id : dico[schem] = dico.Count + 1);
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>())).ReturnsAsync((int id) => dico.Where(x => x.Value == id).First().Key);

            schemaRegistry = schemaRegistryMock.Object;
        }

        [Fact]
        public void GenericSerDe()
        {
            var avroSerializer = new AvroSerializer<object>(schemaRegistry, false);
            IDeserializer<object> avroDeserializer = new AvroDeserializer(schemaRegistry);
            byte[] bytes;
            GenericRecord avroRecord = CreateAvroRecord();
            bytes = avroSerializer.Serialize(topic, avroRecord);
            Assert.Equal(avroRecord, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, null);
            Assert.Equal(null, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, true);
            Assert.Equal(true, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, 123);
            Assert.Equal(123, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, 345L);
            Assert.Equal(345l, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, 1.23f);
            Assert.Equal(1.23f, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, 2.34d);
            Assert.Equal(2.34, avroDeserializer.Deserialize(topic, bytes));

            bytes = avroSerializer.Serialize(topic, "abc");
            Assert.Equal("abc", avroDeserializer.Deserialize(topic, bytes));
            
            bytes = avroSerializer.Serialize(topic, Encoding.UTF8.GetBytes("abc"));
            Assert.Equal("abc", Encoding.UTF8.GetString((byte[])avroDeserializer.Deserialize(topic, bytes)));
        }

        [Fact]
        public void IntSerDe()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistry, false);
            IDeserializer<int> avroDeserializer = new AvroDeserializer1<int>(schemaRegistry);
            byte[] bytes;
            bytes = avroSerializer.Serialize(topic, 123);
            Assert.Equal(123, avroDeserializer.Deserialize(topic, bytes));
        }

        [Fact]
        public void BoolSerDe()
        {
            ISerializer<bool> avroSerializer = new AvroSerializer<bool>(schemaRegistry, false);
            IDeserializer<bool> avroDeserializer = new AvroDeserializer1<bool>(schemaRegistry);
            byte[] bytes;
            bytes = avroSerializer.Serialize(topic, true);
            Assert.Equal(true, avroDeserializer.Deserialize(topic, bytes));
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistry, false);
            IDeserializer<string> avroDeserializer = new AvroDeserializer1<string>(schemaRegistry);
            byte[] bytes;
            bytes = avroSerializer.Serialize(topic, "abc");
            Assert.Equal("abc", avroDeserializer.Deserialize(topic, bytes));
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>(schemaRegistry, false);
            IDeserializer<double> avroDeserializer = new AvroDeserializer<double>(schemaRegistry);
            byte[] bytes;
            bytes = avroSerializer.Serialize(topic, 123d);
            Assert.Equal(123d, avroDeserializer.Deserialize(topic, bytes));
        }


        [Fact]
        public void AllUsages()
        {
            {
                var avroSerializer = new AvroJsonSerializer<string>(Avro.Schema schema, schemaRegistry, false);
                avroSerializer.Serialize(topic, @"{name=""confluent"", ""age""=10}");
                // serialize using the given schema fro topic. throw if schema is not valid
            }

            {
                var avroSerializer = new AvroJsonSerializer<string>(string schema, schemaRegistry, false);
                avroSerializer.Serialize(topic, @"{name=""confluent"", ""age""=10}");
                // serialize using the given schema for topic (as a string, so no avro necessary in code). throw if schema is not valid
            }

            {
                var avroSerializer = new AvroSerializer<User>(schemaRegistry, false);
                avroSerializer.Serialize(topic, @"{name=""confluent"", ""age""=10}");
                // serialize using an ISpecificRecord (and it's embedded schema)
            }

            {
                var avroSerializer = new AvroSerializer<MD5>(schemaRegistry, false);
                avroSerializer.Serialize(topic, @"{name=""confluent"", ""age""=10}");
                // serialize using an ISpecificRecord (and it's embedded schema)
            }

            {
                var avroSerializer = new AvroSerializer<string>(schemaRegistry, false);
                avroSerializer.Serialize(topic, "confluent");
                // serialize using the given schema for topic (as a string, so no avro necessary in code). throw if schema is not valid
            }

            {
                var avroSerializer = new AvroSerializer<string>(schemaRegistry, false);
                avroSerializer.Serialize(topic, "confluent");
                // serialize data using a string schema
            }

            {
                var avroSerializer = new AvroSerializer<GenericRecord>(schemaRegistry, false);
                GenericRecord r = new GenericRecord(schema);
                avroSerializer.Serialize(topic, "confluent");
                // serialize data using a string schema
            }


            // 2 cases:
            // either a specific type, either a generic one


            IDeserializer<double> avroDeserializer = new AvroDeserializer<double>(schemaRegistry);
            byte[] bytes;
            bytes = avroSerializer.Serialize(topic, 123d);
            Assert.Equal(123d, avroDeserializer.Deserialize(topic, bytes));
        }
    }
}
