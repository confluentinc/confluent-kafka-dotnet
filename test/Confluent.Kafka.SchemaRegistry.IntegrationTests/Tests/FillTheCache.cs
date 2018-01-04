using System;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void FillTheCache(string server)
        {
            const int capacity = 16;

            const string testSchema = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", server },
                { "schema.registry.timeout.ms", 3000 },
                { "schema.registry.cache.capacity", capacity }
            };

            var sr = new CachedSchemaRegistryClient(config);

            var registerCount = capacity + 10;

            var subjects = new List<string>();
            var ids = new List<int>();
            for (int i=0; i<registerCount; ++i)
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = sr.ConstructValueSubjectName(topicName);
                subjects.Add(subject);

                var id = sr.RegisterAsync(subject, testSchema).Result;
                ids.Add(id);
            }

            for (int i=0; i<registerCount; ++i)
            {
                var s = sr.GetSchemaAsync(ids[i]).Result;
                Assert.Equal(s, testSchema);
            }

            for (int i=0; i<registerCount; ++i)
            {
                var latest = sr.GetLatestSchemaAsync(subjects[i]).Result;
                Assert.True(latest.Id > 0);
                Assert.Equal(latest.Version, 1);
                Assert.Equal(latest.SchemaString, testSchema);
                Assert.Equal(latest.Subject, subjects[i]);

                var schema = sr.GetSchemaAsync(subjects[i], latest.Version).Result;
                Assert.NotNull(schema);
            }

            // Note: Inspect in debugger for detailed internal cache behavior investigation.
        }
    }
}
