using Moq;
using Xunit;
using Confluent.Kafka.Serialization;
using com.example.tests;

namespace Confluent.Kafka.SchemaRegistry.UnitTests.Serializer
{

    public class KafkaAvroSerializerUnitTest
    {
        [Fact]
        public void SpecificSerializerAndDeserialize()
        {
            var schemaRegistry = new Mock<ISchemaRegistryClient>();
            schemaRegistry.Setup(x => x.GetRegistrySubject("topic", false)).Returns("topic-value");
            schemaRegistry.Setup(x => x.RegisterAsync("topic-value", User._SCHEMA.ToString())).ReturnsAsync(1);
            schemaRegistry.Setup(x => x.GetSchemaAsync(1)).ReturnsAsync(User._SCHEMA.ToString());

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };
            var serializer = new ConfluentAvroSerializer<User>(schemaRegistry.Object, false);
            var deserializer = new ConfluentAvroDeserializer<User>(schemaRegistry.Object);

            var bytes = serializer.Serialize("topic", user);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(user.name, deserUser.name);
            Assert.Equal(user.favorite_color, deserUser.favorite_color);
            Assert.Equal(user.favorite_number, deserUser.favorite_number);
        }

        [Fact]
        public void SpecificSerializerAndDeserialize_Int()
        {
            var schemaRegistry = new Mock<ISchemaRegistryClient>();
            schemaRegistry.Setup(x => x.GetRegistrySubject("topic", false)).Returns("topic-value");
            string intSchema = Avro.Schema.Parse("int").ToString();
            schemaRegistry.Setup(x => x.RegisterAsync("topic-value", intSchema)).ReturnsAsync(1);
            schemaRegistry.Setup(x => x.GetSchemaAsync(1)).ReturnsAsync(intSchema);
            
            var serializer = new ConfluentAvroSerializer<int>(schemaRegistry.Object, false);
            var deserializer = new ConfluentAvroDeserializer<int>(schemaRegistry.Object);

            var bytes = serializer.Serialize("topic", 10);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(10, deserUser);
        }

        [Fact]
        public void SpecificSerializerAndDeserialize_Double()
        {
            var schemaRegistry = new Mock<ISchemaRegistryClient>();
            schemaRegistry.Setup(x => x.GetRegistrySubject("topic", false)).Returns("topic-value");
            string intSchema = Avro.Schema.Parse("int").ToString();
            schemaRegistry.Setup(x => x.RegisterAsync("topic-value", intSchema)).ReturnsAsync(1);
            schemaRegistry.Setup(x => x.GetSchemaAsync(1)).ReturnsAsync(intSchema);

            var serializer = new ConfluentAvroSerializer<int>(schemaRegistry.Object, false);
            var deserializer = new ConfluentAvroDeserializer<int>(schemaRegistry.Object);

            var bytes = serializer.Serialize("topic", 10);
            var deserUser = deserializer.Deserialize("topic", bytes);

            Assert.Equal(10, deserUser);
        }
    }
}
