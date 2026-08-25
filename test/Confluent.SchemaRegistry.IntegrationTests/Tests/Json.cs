// Copyright 2020 Confluent Inc.
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

using System;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {

        // from: https://json-schema.org/learn/getting-started-step-by-step.html
        private static string TestJsonSchema = @"
{
  ""$schema"": ""http://json-schema.org/draft-07/schema#"",
  ""$id"": ""http://example.com/product.schema.json"",
  ""title"": ""Product"",
  ""description"": ""A product from Acme's catalog"",
  ""type"": ""object"",
  ""properties"": {
    ""productId"": {
      ""description"": ""The unique identifier for a product"",
      ""type"": ""integer""
    },
    ""productName"": {
      ""description"": ""Name of the product"",
      ""type"": ""string""
    },
    ""price"": {
      ""description"": ""The price of the product"",
      ""type"": ""number"",
      ""exclusiveMinimum"": 0
    },
    ""tags"": {
      ""description"": ""Tags for the product"",
      ""type"": ""array"",
      ""items"": {
        ""type"": ""string""
      },
      ""minItems"": 1,
      ""uniqueItems"": true
    }
  },
  ""required"": [ ""productId"", ""productName"", ""price"" ]
}";

        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static async Task Json(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName+"2", null);

            var id1 = await srInitial.RegisterSchemaAsync(subjectInitial, new Schema(TestJsonSchema, SchemaType.Json));
            var schema1 = await sr.GetSchemaAsync(id1); // use a different sr instance to ensure a cached value is not read.
            Assert.Equal(SchemaType.Json, schema1.SchemaType);
            Assert.NotNull(schema1.SchemaString); // SR munges the schema (whitespace), so in general this won't equal the registered schema.

            // check that the id of the schema just registered can be retrieved.
            var id = await sr.GetSchemaIdAsync(subjectInitial, new Schema(schema1.SchemaString, SchemaType.Json));
            Assert.Equal(id1, id);

            // re-register the munged schema (to a different subject) and check that it is not re-munged.
            var id2 = await sr.RegisterSchemaAsync(subject, schema1);
            var schema2 = await sr.GetSchemaAsync(id2);
            Assert.Equal(schema1.SchemaString, schema2.SchemaString);
            Assert.Equal(schema1.SchemaType, schema2.SchemaType);

            // compatibility
            var compat = await sr.IsCompatibleAsync(subject, schema2);
            Assert.True(compat);
            var avroSchema = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var compat2 = await sr.IsCompatibleAsync(subject, avroSchema);
            Assert.False(compat2);
            var compat3 = await sr.IsCompatibleAsync(subject, new Schema(avroSchema, SchemaType.Avro));
            Assert.False(compat3);
        }
    }
}
