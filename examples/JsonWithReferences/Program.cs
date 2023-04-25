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

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using NJsonSchema;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema.Generation;


/// <summary>
///     An example of working with JSON data, Apache Kafka and 
///     Confluent Schema Registry (v5.5 or later required for
///     JSON schema support).
/// </summary>
namespace Confluent.Kafka.Examples.JsonWithReferences
{
    /// <summary>
    ///     A POCO class corresponding to the JSON data written
    ///     to Kafka, where the schema is defined with external
    ///     references to other schemas
    /// </summary>
    /// <remarks>
    ///     Internally, the JSON serializer uses Newtonsoft.Json for
    ///     serialization and NJsonSchema for schema creation and
    ///     validation. You can use any property annotations recognised
    ///     by these libraries.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the Confluent serializers.
    /// </remarks>

    class Program
    {
        // from: https://json-schema.org/learn/getting-started-step-by-step.html
        private static string S1;
        private static string S2;
        public static Dictionary<string, JsonSchema> dict = new Dictionary<string, JsonSchema>();
        public static Dictionary<string, Schema> schema_strs = new Dictionary<string, Schema>();
        public static JsonSchema getSchema(Schema root)
        {
            List<SchemaReference> refers = root.References;
            foreach (var x in refers)
            {
                dict.Add(x.Name, getSchema(schema_strs[x.Name]));
            }

            Func<JsonSchema, JsonReferenceResolver> factory;
            factory = x =>
            {
                JsonSchemaResolver schemaResolver = new JsonSchemaResolver(x, new JsonSchemaGeneratorSettings());
                JsonReferenceResolver referenceResolver = new JsonReferenceResolver(schemaResolver);
                foreach (var reference in refers)
                {
                    JsonSchema jschema = dict[reference.Name];
                    referenceResolver.AddDocumentReference(reference.Name, jschema);
                }
                return referenceResolver;
            };

            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            JsonSchema root_schema = JsonSchema.FromJsonAsync(root_str, schemaId, factory).Result;
            return root_schema;
        }
        static async Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

            S1 = File.ReadAllText("geographical-location.json");
            S2 = File.ReadAllText("product.json");
            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SaslUsername = "broker",
                SaslPassword = "broker",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.ScramSha256
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };

            var srInitial = new CachedSchemaRegistryClient(schemaRegistryConfig);
            var sr = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject1 = "ruchir-CoordinatesOnMap";
            var subject2 = "ruchir-Product";

            // Test there are no errors (exceptions) registering a schema that references another.
            var id1 = srInitial.RegisterSchemaAsync(subject1, new Schema(S1, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s1 = srInitial.GetLatestSchemaAsync(subject1).Result;
            var refs = new List<SchemaReference> { new SchemaReference("geographical-location.json", subject1, s1.Version) };
            var id2 = srInitial.RegisterSchemaAsync(subject2, new Schema(S2, refs, Confluent.SchemaRegistry.SchemaType.Json)).Result;

            // In fact, it seems references are not checked server side.
            var latestSchema2 = sr.GetLatestSchemaAsync(subject2).Result;
            var latestSchema2_unreg = latestSchema2.Schema;
            var latestSchema1 = sr.GetLatestSchemaAsync(subject1).Result;
            schema_strs.Add("geographical-location.json", latestSchema1);
            schema_strs.Add("product.json", latestSchema2);

            JsonSchema s2_schema = getSchema(latestSchema2);
            //System.Console.WriteLine(s2_schema.ToJson());

            var jsonSerializerConfig = new JsonSerializerConfig
            {
                BufferBytes = 100,
                UseLatestVersion = true,
                AutoRegisterSchemas = false,
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            var jsonSerializer = new JsonSerializer<Object>(sr, latestSchema2);

            var obje = new
            {
                productId = 123,
                productName = "Example product",
                price = 9.99,
                tags = new List<string> { "tag1", "tag2" },
                dimensions = new
                {
                    length = 10.0,
                    width = 5.0,
                    height = 2.0
                },
                warehouseLocation = new
                {
                    latitude = 37.7749,
                    longitude = -122.4194
                }
            };

            string json = JsonConvert.SerializeObject(obje);

            var jObject = JsonConvert.DeserializeObject<JObject>(json);
            //System.Console.WriteLine(jObject?["productName"]?.ToString() ?? "");
            var validationResult = s2_schema.Validate(jObject);

            if (validationResult.Count > 0)
            {
                throw new InvalidDataException("Schema validation failed for properties: [" + string.Join(", ", validationResult.Select(r => r.Path)) + "]");
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, Object>(producerConfig)
                    .SetValueSerializer(new JsonSerializer<Object>(schemaRegistry, latestSchema2_unreg, jsonSerializerConfig))
                    .Build())
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter first product names, q to exit.");

                long i = 1;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var obj = new
                    {
                        productId = i++,
                        productName = text,
                        price = 9.99,
                        tags = new List<string> { "tag1", "tag2" },
                        dimensions = new
                        {
                            length = 10.0,
                            width = 5.0,
                            height = 2.0
                        },
                        warehouseLocation = new
                        {
                            latitude = 37.7749,
                            longitude = -122.4194
                        }
                    };
                    try
                    {
                        await producer.ProduceAsync(topicName, new Message<string, Object> { Value = obj });
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"error producing message: {e.Message}");
                    }
                }
            }
        }
    }
}
