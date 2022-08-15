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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using System;
using Avro.Generic;

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
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) => new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Avro)
            );
            schemaRegistryClient = schemaRegistryMock.Object;
        }

        [Fact]
        public void IntSerDe()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(1234, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(1234, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void LongSerDe()
        {
            var avroSerializer = new AvroSerializer<long>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<long>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void BoolSerDe()
        {
            var avroSerializer = new AvroSerializer<bool>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<bool>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(true, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<string>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync("abc", new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal("abc", avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<double>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123d, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123d, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void FloatSerDe()
        {
            var avroSerializer = new AvroSerializer<float>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<float>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123f, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123f, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void BytesSerDe()
        {
            var avroSerializer = new AvroSerializer<byte[]>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<byte[]>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(new byte[] { 2, 3, 4 }, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(new byte[] { 2, 3, 4 }, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void NullSerDe()
        {
            var avroSerializer = new AvroSerializer<Null>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<Null>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(null, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
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

            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void NullISpecificRecord()
        {
            var serializer = new AvroSerializer<User>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullGenericRecord()
        {
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullString()
        {
            var serializer = new AvroSerializer<string>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<string>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullInt()
        {
            var deserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var exception = Assert.Throws<AggregateException>(() => deserializer.DeserializeAsync(ReadOnlyMemory<byte>.Empty, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);

            Assert.Equal("Cannot deserialize null to a Value Type", exception.InnerException.Message);
        }

        [Fact]
        public void Multiple_ISpecificRecords()
        {
            var serializer = new AvroSerializer<ISpecificRecord>(schemaRegistryClient);
            var deserializerUser = new AvroDeserializer<User>(schemaRegistryClient);
            var deserializerCar = new AvroDeserializer<Car>(schemaRegistryClient);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var car = new Car
            {
                color = "blue",
                name = "great_brand"
            };

            var bytesUser = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var resultUser = deserializerUser.DeserializeAsync(bytesUser, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result as User;

            Assert.NotNull(resultUser);
            Assert.Equal(user.name, resultUser.name);
            Assert.Equal(user.favorite_color, resultUser.favorite_color);
            Assert.Equal(user.favorite_number, resultUser.favorite_number);

            var bytesCar = serializer.SerializeAsync(car, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var resultCar = deserializerCar.DeserializeAsync(bytesCar, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result as Car;

            Assert.NotNull(resultCar);
            Assert.Equal(car.name, resultCar.name);
            Assert.Equal(car.color, resultCar.color);
        }

        [Fact]
        public void Poco_Serialize()
        {
            var serializer = new AvroSerializer<Dictionary<string, string>>(schemaRegistryClient);
            Assert.Throws<System.InvalidOperationException>(() => serializer.SerializeAsync(new Dictionary<string, string> { { "cat", "dog" } }, new SerializationContext(MessageComponentType.Key, testTopic)).GetAwaiter().GetResult());
        }

        [Fact]
        public void Poco_Deserialize()
        {
            var deserializer = new AvroDeserializer<Dictionary<string, string>>(schemaRegistryClient);
            Assert.Throws<System.InvalidOperationException>(() => deserializer.DeserializeAsync(new System.ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }), false, new SerializationContext(MessageComponentType.Key, testTopic)).GetAwaiter().GetResult());
        }

        [Fact]
        public void Incompatible()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            var bytes = avroSerializer.SerializeAsync("hello world", new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Throws<System.AggregateException>(() => avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        /// <summary>
        /// Test a case when .NET data class name and / or namespace do not match the schema name and / or namespace.
        /// </summary>
        [Fact]
        public void ISpecificRecord_SchemaTypeMismatch()
        {
            var serializer = new AvroSerializer<User2>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User2>(schemaRegistryClient);

            var user = new User2
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }
    }
}
