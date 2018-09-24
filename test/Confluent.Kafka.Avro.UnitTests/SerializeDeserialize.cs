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
using Confluent.Kafka.Serialization;
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
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, 123);
            Assert.Equal(123, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void LongSerDe()
        {
            var avroSerializer = new AvroSerializer<long>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<long>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, 123);
            Assert.Equal(123, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void BoolSerDe()
        {
            var avroSerializer = new AvroSerializer<bool>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<bool>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, true);
            Assert.Equal(true, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<string>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, "abc");
            Assert.Equal("abc", avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<double>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, 123d);
            Assert.Equal(123d, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void FloatSerDe()
        {
            var avroSerializer = new AvroSerializer<float>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<float>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, 123f);
            Assert.Equal(123f, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void BytesSerDe()
        {
            var avroSerializer = new AvroSerializer<byte[]>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<byte[]>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, new byte[] { 2, 3, 4 });
            Assert.Equal(new byte[] { 2, 3, 4 }, avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void NullSerDe()
        {
            var avroSerializer = new AvroSerializer<Null>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<Null>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, null);
            Assert.Null(avroDeserializer.Deserialize(testTopic, bytes, false));
        }

        [Fact]
        public void ISpecificRecord()
        {
            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient);
            serializer.Configure(new Dictionary<string, string>(), false);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient);
            deserializer.Configure(new Dictionary<string, string>(), false);

            var bytes = serializer.Serialize("topic", user);
            var result = deserializer.Deserialize("topic", bytes, false);

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void Incompatible()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            avroSerializer.Configure(new Dictionary<string, string>(), false);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            avroDeserializer.Configure(new Dictionary<string, string>(), false);
            byte[] bytes;
            bytes = avroSerializer.Serialize(testTopic, "Hello world");
            Assert.Throws<global::Avro.AvroException>(() => avroDeserializer.Deserialize(testTopic, bytes, false));
        }
    }
}
