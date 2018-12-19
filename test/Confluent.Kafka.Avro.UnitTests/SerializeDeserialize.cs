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
using System.Collections.Generic;
using System.Linq;
using Confluent.SchemaRegistry;
using Confluent.Kafka.AvroSerdes;
using Confluent.Kafka.Examples.AvroSpecific;


namespace Confluent.Kafka.Avro.UnitTests
{
    public class SerializeDeserialzeTests
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public SerializeDeserialzeTests()
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
        public void IntSerDe()
        {
            var avroSerializer = new AvroSerializer<int>();
            var avroDeserializer = new AvroDeserializer<int>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, 123, false).Result;
            Assert.Equal(123, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void LongSerDe()
        {
            var avroSerializer = new AvroSerializer<long>();
            var avroDeserializer = new AvroDeserializer<long>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, 123, false).Result;
            Assert.Equal(123, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void BoolSerDe()
        {
            var avroSerializer = new AvroSerializer<bool>();
            var avroDeserializer = new AvroDeserializer<bool>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, true, false).Result;
            Assert.Equal(true, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>();
            var avroDeserializer = new AvroDeserializer<string>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, "abc", false).Result;
            Assert.Equal("abc", avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>();
            var avroDeserializer = new AvroDeserializer<double>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, 123d, false).Result;
            Assert.Equal(123d, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void FloatSerDe()
        {
            var avroSerializer = new AvroSerializer<float>();
            var avroDeserializer = new AvroDeserializer<float>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, 123f, false).Result;
            Assert.Equal(123f, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void BytesSerDe()
        {
            var avroSerializer = new AvroSerializer<byte[]>();
            var avroDeserializer = new AvroDeserializer<byte[]>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, new byte[] { 2, 3, 4 }, false).Result;
            Assert.Equal(new byte[] { 2, 3, 4 }, avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void NullSerDe()
        {
            var avroSerializer = new AvroSerializer<Null>();
            var avroDeserializer = new AvroDeserializer<Null>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, null, false).Result;
            Assert.Null(avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }

        [Fact]
        public void ISpecificRecord()
        {
            var serializer = new AvroSerializer<User>();
            var deserializer = new AvroDeserializer<User>();

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.Serialize(schemaRegistryClient, "topic", user, false).Result;
            var result = deserializer.Deserialize(schemaRegistryClient, "topic", bytes, false).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void Incompatible()
        {
            var avroSerializer = new AvroSerializer<string>();
            var avroDeserializer = new AvroDeserializer<int>();
            byte[] bytes;
            bytes = avroSerializer.Serialize(schemaRegistryClient, testTopic, "Hello world", false).Result;
            Assert.Throws<System.AggregateException>(() => avroDeserializer.Deserialize(schemaRegistryClient, testTopic, bytes, false).Result);
        }
    }
}
