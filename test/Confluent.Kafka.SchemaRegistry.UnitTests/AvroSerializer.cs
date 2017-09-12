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
using Confluent.Kafka.SchemaRegistry.Serialization;
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


        [Fact]
        public void GenericSerDe()
        {
            var dico = new Dictionary<string, int>();
            string topic = "topic";
            var schemaRegistry = new Mock<ISchemaRegistryClient>();
            schemaRegistry.Setup(x => x.GetRegistrySubject(topic, false)).Returns("topic-value");
            schemaRegistry.Setup(x => x.RegisterAsync("topic-value", It.IsAny<string>())).ReturnsAsync((string top, string schem) => dico.TryGetValue(schem, out int id) ? id : dico[schem] = dico.Count+1);
            schemaRegistry.Setup(x => x.GetSchemaAsync(It.IsAny<int>())).ReturnsAsync((int id) => dico.Where(x => x.Value == id).First().Key);

            var avroSerializer = new AvroSerializer(schemaRegistry.Object, false);
            var avroDeserializer = new AvroDeserializer(schemaRegistry.Object);
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

    }
}
