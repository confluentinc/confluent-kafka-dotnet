// Copyright 2023 Confluent Inc.
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
using Confluent.Kafka.SyncOverAsync;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Linq;
using NJsonSchema.Generation;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        class Order
        {
            public DateTime OrderDate {get; set;}
            
            public OrderDetails OrderDetails {get; set;}
        }
        
        class OrderDetails
        {
            public int Id {get; set;}
            
            public Customer Customer {get; set;}
            
            public string PaymentId {get; set;}
        }
        
        class Customer
        {
            public int Id {get; set;}

            public string Name {get; set;}
            
            public string Email {get; set;}
        }
        
        private static string Schema1 = @"
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

        private static string Schema2 = @"
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
    ""required"": [""order_details""]
}";

        private static string Schema3 = @"
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

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };
            var sr = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var jsonSchemaGeneratorSettings = new JsonSchemaGeneratorSettings
            {
                SerializerSettings = new JsonSerializerSettings
                {
                    ContractResolver = new DefaultContractResolver
                    {
                        NamingStrategy = new SnakeCaseNamingStrategy()
                    }
                }
            };

            // Register the reference schemas
            var subject3 = "Customer";
            var id3 = sr.RegisterSchemaAsync(subject3, new(Schema3, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s3 = sr.GetLatestSchemaAsync(subject3).Result;

            var subject1 = "OrderDetails";
            var refs1 = new List<SchemaReference> { new("http://example.com/customer.schema.json", subject3, s3.Version) };
            var id1 = sr.RegisterSchemaAsync(subject1, new(Schema1, refs1, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s1 = sr.GetLatestSchemaAsync(subject1).Result;

            // Register the top level schema
            var subject2 = "Order";
            var refs2 = new List<SchemaReference> { new("http://example.com/order_details.schema.json", subject1, s1.Version) };
            var id2 = sr.RegisterSchemaAsync(subject2, new(Schema2, refs2, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s2 = sr.GetLatestSchemaAsync(subject2).Result;

            // Create serialiser and deserialiser along with the Order schema
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                var order = new Order
                {
                    OrderDetails = new OrderDetails
                    {
                        Id = 123,
                        Customer = new Customer
                        {
                            Name = "Schema Registry",
                            Id = 456,
                            Email = "schema@example.com"
                        },
                        PaymentId = "abc123"
                    },
                    OrderDate = DateTime.UtcNow
                };
                
                using (var producer =
                    new ProducerBuilder<string, Order>(producerConfig)
                        .SetValueSerializer(new JsonSerializer<Order>(schemaRegistry, s2.Schema,
                            jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings))
                        .Build())
                {
                    producer.ProduceAsync(topic.Name, new Message<string, Order> { Key = "test1", Value = order }).Wait();
                }
                
                using (var consumer =
                new ConsumerBuilder<string, Order>(consumerConfig)
                    .SetValueDeserializer(new JsonDeserializer<Order>(sr, s2.Schema,
                            jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings).AsSyncOverAsync())
                    .Build())
                {
                    consumer.Subscribe(topic.Name);
                    var cr = consumer.Consume();
                    var classObj = cr.Message.Value;
                    Assert.Equal<int>(123, classObj.OrderDetails.Id);
                }

                // Test producing and consuming directly a JObject
                var serializedString = Newtonsoft.Json.JsonConvert.SerializeObject(order,
                    jsonSchemaGeneratorSettings.ActualSerializerSettings);
                var jsonObject = JObject.Parse(serializedString);
                
                using (var producer =
                    new ProducerBuilder<string, JObject>(producerConfig)
                        .SetValueSerializer(new JsonSerializer<JObject>(schemaRegistry, s2.Schema,
                            jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings))
                        .Build())
                {
                    producer.ProduceAsync(topic.Name, new Message<string, JObject> { Key = "test1", Value = jsonObject }).Wait();
                }
                
                using (var consumer =
                new ConsumerBuilder<string, JObject>(consumerConfig)
                    .SetValueDeserializer(new JsonDeserializer<JObject>(sr, s2.Schema,
                            jsonSchemaGeneratorSettings: jsonSchemaGeneratorSettings).AsSyncOverAsync())
                    .Build())
                {
                    consumer.Subscribe(topic.Name);
                    var cr = consumer.Consume();
                    var classObj = cr.Message.Value;
                    Assert.Equal(123, classObj["order_details"]?["id"]?.Value<int>() ?? 0);
                }

                // Test producing with a different class
                using (var producer =
                    new ProducerBuilder<string, TestClasses2.TestPoco>(producerConfig)
                        .SetValueSerializer(new JsonSerializer<TestClasses2.TestPoco>(
                            schemaRegistry, s2.Schema, new JsonSerializerConfig
                            {
                                UseLatestVersion = true,
                                AutoRegisterSchemas = false,
                                LatestCompatibilityStrict = true
                            },
                            jsonSchemaGeneratorSettings))
                        .Build())
                {
                    var c = new Confluent.SchemaRegistry.Serdes.IntegrationTests.TestClasses2.TestPoco { StringField = "Test" };
                    // Validation failure when passing TestClasses2.TestPoco
                    Assert.Throws<AggregateException>(
                        () => producer.ProduceAsync(topic.Name, new Message<string, TestClasses2.TestPoco> { Key = "test1", Value = c }).Wait());
                }
            }
        }
    }
}
