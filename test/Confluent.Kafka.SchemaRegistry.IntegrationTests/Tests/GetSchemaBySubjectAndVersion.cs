using System;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void GetSchemaBySubjectAndVersion(string server)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>{ { "schema.registry.urls", server } });

            var subject = sr.ConstructValueSubjectName(topicName);
            var id = sr.RegisterAsync(subject, testSchema1).Result;

            var schema = sr.GetLatestSchemaAsync(subject).Result;
            var schemaString = sr.GetSchemaAsync(subject, schema.Version).Result;

            Assert.Equal(schemaString, testSchema1);
        }
    }
}
