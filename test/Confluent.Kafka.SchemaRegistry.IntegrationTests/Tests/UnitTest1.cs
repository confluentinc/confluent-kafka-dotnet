using System;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void Test1(string server)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroGeneric" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(server);

            var subject = sr.ConstructRegistrySubject(topicName, SubjectType.Key);
            Assert.Equal(topicName + "-key", subject);

            var id1 = sr.RegisterAsync(subject, testSchema1).Result;
            var id2 = sr.RegisterAsync(subject, testSchema1).Result;

            Assert.Equal(id1, id2);

            var testSchema2 = // incompatible with testSchema1
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroGeneric" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_shape\",\"type\":[\"string\",\"null\"]}]}";

            Assert.False(sr.IsCompatibleAsync(subject, testSchema2).Result);

            Assert.Throws<AggregateException>(() => sr.RegisterAsync(subject, testSchema2).Result);

            var ss = sr.GetAllSubjectsAsync().Result;
        }
    }
}
