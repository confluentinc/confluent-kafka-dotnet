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
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Examples.AvroSpecific;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
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
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(123, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void LongSerDe()
        {
            var avroSerializer = new AvroSerializer<long>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<long>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(123, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void BoolSerDe()
        {
            var avroSerializer = new AvroSerializer<bool>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<bool>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(true, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(true, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<string>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync("abc", false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal("abc", avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<double>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123d, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(123d, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void FloatSerDe()
        {
            var avroSerializer = new AvroSerializer<float>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<float>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123f, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(123f, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void BytesSerDe()
        {
            var avroSerializer = new AvroSerializer<byte[]>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<byte[]>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(new byte[] { 2, 3, 4 }, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(new byte[] { 2, 3, 4 }, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void NullSerDe()
        {
            var avroSerializer = new AvroSerializer<Null>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<Null>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(null, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result;
            Assert.Equal(null, avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition(testTopic, Partition.Any)).Result);
        }

        [Fact]
        public void ISpecificRecord()
        {
            var serializer = new AvroSerializer<User>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.SerializeAsync(user, false, new MessageMetadata(), new TopicPartition("topic", Partition.Any)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition("topic", Partition.Any)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void Incompatible()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            var bytes = avroSerializer.SerializeAsync("hello world", false, new MessageMetadata(), new TopicPartition("topic", Partition.Any)).Result;
            Assert.Throws<System.AggregateException>(() => avroDeserializer.DeserializeAsync(bytes, false, false, new MessageMetadata(), new TopicPartition("topic", Partition.Any)).Result);
        }
    }
}
