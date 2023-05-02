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

using Xunit;
using System;
using System.Collections.Generic;
using Confluent.Kafka;
using NJsonSchema;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema.Generation;

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        private static string S1 = @"
{
""$schema"": ""http://json-schema.org/draft-07/schema#"",
""$id"": ""http://example.com/order_details.schema.json"",
""title"": ""OrderDetails"",
""description"": ""Order Details"",
""type"": ""object"",
""properties"": {
""id"": {
""description"": ""Order Id"",
""type"": ""integer""
},
""customer"": {
""description"": ""Customer"",
""$ref"": ""http://example.com/customer.schema.json""
},
""payment_id"": {
""description"": ""Payment Id"",
""type"": ""string""
}
},
""required"": [ ""id"", ""customer""]
}";

        private static string S2 = @"
{
""$schema"": ""http://json-schema.org/draft-07/schema#"",
""$id"": ""http://example.com/referencedproduct.schema.json"",
""title"": ""Order"",
""description"": ""Order"",
""type"": ""object"",
""properties"": {
""order_details"": {
""description"": ""Order Details"",
""$ref"": ""http://example.com/order_details.schema.json""
},
""order_date"": {
""description"": ""Order Date"",
""type"": ""string"",
""format"": ""date-time""
}
},
""required"": [
""order_details""]
}";

        private static string S3 = @"
{
""$schema"": ""http://json-schema.org/draft-07/schema#"",
""$id"": ""http://example.com/customer.schema.json"",
""title"": ""Customer"",
""description"": ""Customer Data"",
""type"": ""object"",
""properties"": {
""name"": {
""Description"": ""Customer name"",
""type"": ""string""
},
""id"": {
""description"": ""Customer id"",
""type"": ""integer""
},
""email"": {
""description"": ""Customer email"",
""type"": ""string""
}
},
""required"": [ ""name"", ""id""]
}";
        /// <summary>
        ///     Test Use References. 
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void UseReferences(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };
            var sr = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var subject3 = "Customer";
            var id3 = sr.RegisterSchemaAsync(subject3, new Schema(S3, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s3 = sr.GetLatestSchemaAsync(subject3).Result;

            var subject1 = "OrderDetails";
            var refs1 = new List<SchemaReference> { new SchemaReference("http://example.com/customer.schema.json", subject3, s3.Version) };
            var id1 = sr.RegisterSchemaAsync(subject1, new Schema(S1, refs1, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s1 = sr.GetLatestSchemaAsync(subject1).Result;

            var subject2 = "Order";
            var refs2 = new List<SchemaReference> { new SchemaReference("http://example.com/order_details.schema.json", subject1, s1.Version) };
            var id2 = sr.RegisterSchemaAsync(subject2, new Schema(S2, refs2, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s2 = sr.GetLatestSchemaAsync(subject2).Result;

            var jsonSerializer = new JsonSerializer<Object>(sr, s2.Schema);
            // using (var topic = new TemporaryTopic(bootstrapServers, 1))
            // using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig)) 
            // {
            //     using (var producer =
            //         new ProducerBuilder<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco>(producerConfig)
            //             .SetValueSerializer(new JsonSerializer<Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco>(schemaRegistry))
            //             .Build())
            //     {
            //         var c = new Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco { IntField = 1 };
            //         producer.ProduceAsync(topic.Name, new Message<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses1.TestPoco> { Key = "test1", Value = c }).Wait();
            //     }

            //     using (var producer = 
            //         new ProducerBuilder<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco>(producerConfig)
            //             .SetValueSerializer(new JsonSerializer<Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco>(
            //                 schemaRegistry, new JsonSerializerConfig { UseLatestVersion = true, AutoRegisterSchemas = false, LatestCompatibilityStrict = true }))
            //             .Build())
            //     {
            //         var c = new Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco { StringField = "Test" };
            //         Assert.Throws<AggregateException>(
            //             () => producer.ProduceAsync(topic.Name, new Message<string, Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco> { Key = "test1", Value = c }).Wait());
            //     }
            // }
        }
    }
}
