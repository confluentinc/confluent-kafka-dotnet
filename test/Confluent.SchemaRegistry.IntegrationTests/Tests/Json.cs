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
        public static void Json(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName+"2", null);

            var id1 = srInitial.RegisterSchemaAsync(subjectInitial, new Schema(TestJsonSchema, SchemaType.Json)).Result;
            var schema1 = sr.GetSchemaAsync(id1).Result; // use a different sr instance to ensure a cached value is not read.
            Assert.Equal(SchemaType.Json, schema1.SchemaType);
            Assert.NotNull(schema1.SchemaString); // SR munges the schema (whitespace), so in general this won't equal the registered schema.

            // check that the id of the schema just registered can be retrieved.
            var id = sr.GetSchemaIdAsync(subjectInitial, new Schema(schema1.SchemaString, SchemaType.Json)).Result;
            Assert.Equal(id1, id);

            // re-register the munged schema (to a different subject) and check that it is not re-munged.
            var id2 = sr.RegisterSchemaAsync(subject, schema1).Result;
            var schema2 = sr.GetSchemaAsync(id2).Result;
            Assert.Equal(schema1.SchemaString, schema2.SchemaString);
            Assert.Equal(schema1.SchemaType, schema2.SchemaType);

            // compatibility
            var compat = sr.IsCompatibleAsync(subject, schema2).Result;
            Assert.True(compat);
            var avroSchema = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var compat2 = sr.IsCompatibleAsync(subject, avroSchema).Result;
            Assert.False(compat2);
            var compat3 = sr.IsCompatibleAsync(subject, new Schema(avroSchema, SchemaType.Avro)).Result;
            Assert.False(compat3);
        }
    }
}
