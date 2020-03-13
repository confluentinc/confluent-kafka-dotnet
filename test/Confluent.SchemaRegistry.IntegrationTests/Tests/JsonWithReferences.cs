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
using System.Collections.Generic;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        // from: https://json-schema.org/learn/getting-started-step-by-step.html
        private static string S1 = @"
{
  ""$id"": ""https://example.com/geographical-location.schema.json"",
  ""$schema"": ""http://json-schema.org/draft-07/schema#"",
  ""title"": ""Longitude and Latitude"",
  ""description"": ""A geographical coordinate on a planet (most commonly Earth)."",
  ""required"": [ ""latitude"", ""longitude"" ],
  ""type"": ""object"",
  ""properties"": {
    ""latitude"": {
      ""type"": ""number"",
      ""minimum"": -90,
      ""maximum"": 90
    },
    ""longitude"": {
      ""type"": ""number"",
      ""minimum"": -180,
      ""maximum"": 180
    }
  }
}";

    private static string S2 = @"
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
    },
    ""dimensions"": {
      ""type"": ""object"",
      ""properties"": {
        ""length"": {
          ""type"": ""number""
        },
        ""width"": {
          ""type"": ""number""
        },
        ""height"": {
          ""type"": ""number""
        }
      },
      ""required"": [ ""length"", ""width"", ""height"" ]
    },
    ""warehouseLocation"": {
      ""description"": ""Coordinates of the warehouse where the product is located."",
      ""$ref"": ""https://example.com/geographical-location.schema.json""
    }
  },
  ""required"": [ ""productId"", ""productName"", ""price"" ]
}";


        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void JsonWithReferences(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject1 = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName + "1", null);
            var subject2 = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName + "2", null);

            // Test there are no errors (exceptions) registering a schema that references another.
            var id1 = srInitial.RegisterSchemaAsync(subject1, new Schema(S1, SchemaType.Json)).Result;
            var s1 = srInitial.GetLatestSchemaAsync(subject1).Result;
            var refs = new List<SchemaReference> { new SchemaReference("https://example.com/geographical-location.schema.json", subject1, s1.Version) };
            var id2 = srInitial.RegisterSchemaAsync(subject2, new Schema(S2, refs, SchemaType.Json)).Result;

            // In fact, it seems references are not checked server side.

            var latestSchema = sr.GetLatestSchemaAsync(subject2).Result;
        }
    }
}
