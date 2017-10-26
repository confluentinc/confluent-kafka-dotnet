﻿using Moq;
using Xunit;
using Confluent.Kafka.Serialization;
using com.example.tests;
using Avro.Specific;

namespace Confluent.Kafka.SchemaRegistry.UnitTests.Serializer
{

    public class KafkaAvroSerializerUnitTest
    {
        private Mock<ISchemaRegistryClient> schemaRegistryMock;
        private ISchemaRegistryClient schemaRegistry;

        private void InitSchemaRegistry(string schema, bool isKey = false, int schemaId = 1, string topic = "topic")
        {
            string subject = $"{topic}-{(isKey ? "key" : "value")}";
            if (schemaRegistryMock == null)
            {
                schemaRegistryMock = new Mock<ISchemaRegistryClient>();
                schemaRegistry = schemaRegistryMock.Object;
                schemaRegistryMock.Setup(x => x.GetRegistrySubject(topic, isKey)).Returns(subject);
            }
            schemaRegistryMock.Setup(x => x.RegisterAsync(subject, schema)).ReturnsAsync(schemaId);
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(schemaId)).ReturnsAsync(schema);
        }

        public void SpecificSerializerAndDeserialize()
        {
            InitSchemaRegistry(User._SCHEMA.ToString());
            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };
            var serializer = new ConfluentAvroSerializer<User>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<User>(schemaRegistry);

            var bytes = serializer.Serialize("topic", user);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(user.name, deserUser.name);
            Assert.Equal(user.favorite_color, deserUser.favorite_color);
            Assert.Equal(user.favorite_number, deserUser.favorite_number);
        }


        [Theory]
        [InlineData(null)]
        [InlineData("test")]
        public void SpecificSerializerAndDeserialize_Union(string text)
        {
            InitSchemaRegistry(Avro.Schema.Parse("[\"null\", \"string\"]").ToString(), false);

            var serializer = new ConfluentAvroSerializer<string>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<string>(schemaRegistry);

            var bytes = serializer.Serialize("topic", text);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(text, deserUser);
        }

        [Fact]
        public void SpecificSerializerAndDeserialize_Int()
        {
            InitSchemaRegistry(Avro.Schema.Parse("int").ToString());

            var serializer = new ConfluentAvroSerializer<int>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<int>(schemaRegistry);

            var bytes = serializer.Serialize("topic", 10);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(10, deserUser);
        }

        [Fact]
        public void SpecificSerializerAndDeserialize_Bool()
        {
            InitSchemaRegistry(Avro.Schema.Parse("boolean").ToString());

            var serializer = new ConfluentAvroSerializer<bool>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<bool>(schemaRegistry);

            var bytes = serializer.Serialize("topic", true);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(true, deserUser);
        }

        [Fact]
        public void SpecificSerializerAndDeserialize_Double()
        {
            InitSchemaRegistry(Avro.Schema.Parse("double").ToString(), false);

            var serializer = new ConfluentAvroSerializer<double>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<double>(schemaRegistry);

            var bytes = serializer.Serialize("topic", 10.5d);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(10.5d, deserUser);
        }

        [Theory]
        [InlineData("")]
        [InlineData("test")]
        public void SpecificSerializerAndDeserialize_String(string text)
        {
            InitSchemaRegistry(Avro.Schema.Parse("string").ToString(), false);

            var serializer = new ConfluentAvroSerializer<string>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<string>(schemaRegistry);

            var bytes = serializer.Serialize("topic", text);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(text, deserUser);
        }
        
        [Fact]
        public void SpecificSerializerAndDeserialize_ByteArray()
        {
            InitSchemaRegistry(Avro.Schema.Parse("bytes").ToString(), false);

            var serializer = new ConfluentAvroSerializer<byte[]>(schemaRegistry, false);
            var deserializer = new ConfluentAvroDeserializer<byte[]>(schemaRegistry);

            var obj = new byte[] { 1, 2, 3 };
            var bytes = serializer.Serialize("topic", obj);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(obj, deserUser);
        }
    }
}
