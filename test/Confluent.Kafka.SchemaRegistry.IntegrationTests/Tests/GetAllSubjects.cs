using System;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void GetAllSubjects(string server)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>{ { "schema.registry.urls", server } });

            var subjectsBefore = sr.GetAllSubjectsAsync().Result;

            var subject = sr.ConstructKeySubjectName(topicName);
            var id = sr.RegisterAsync(subject, testSchema1).Result;

            var subjectsAfter = sr.GetAllSubjectsAsync().Result;

            Assert.Equal(1, subjectsAfter.Count - subjectsBefore.Count);

            sr.RegisterAsync(subject, testSchema1).Wait();

            var subjectsAfter2 = sr.GetAllSubjectsAsync().Result;

            Assert.Equal(subjectsAfter.Count, subjectsAfter2.Count);

            Assert.True(subjectsAfter2.Contains(subject));
        }
    }
}
