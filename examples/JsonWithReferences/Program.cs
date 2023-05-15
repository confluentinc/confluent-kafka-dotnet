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

using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;


/// <summary>
///     An example of working with JSON schemas with external,
///     references and Json data, Apache Kafka and 
///     Confluent Schema Registry (v5.5 or later required for
///     JSON schema support).
/// </summary>
namespace Confluent.Kafka.Examples.JsonWithReferences
{

    // public class Product
    // {
    //     public int ProductId { get; set; }
    //     public string ProductName { get; set; }
    //     public GeographicalLocation WarehouseLocation { get; set; }
    // }

    /// <remarks>
    ///     The deserializer allows multiple ways to consume data.
    /// 
    ///     If the consumer is aware of the entire schema details,
    ///     they can create a class corresponding to it and use the
    ///     deserializer in these ways:
    ///     - without passing a schema, the deserializer will convert
    ///       the serialized string to the object of this class.
    ///     - pass a schema and allow validating against it.
    ///
    ///     If the user is aware of some parts of the schema, they can
    ///     create and pass a function that converts the json string to
    ///     an object of the class corresponding to the parts of schema
    ///     the user is aware of (comment this class and uncomment the
    ///     Product class above, pass Func<string, Product> Convertor)
    ///     to constructor of deserializer.
    ///
    ///     Note: The user can also pass JObject to the 
    ///     ConsumerBuilder<string, JObject> and JsonDeserializer<JObject>
    ///     in order to get JObject instead in consumer.
    /// </remarks>
    public class Product
    {
        public int ProductId { get; set; }
        public string ProductName { get; set; }
        public decimal Price { get; set; }
        public List<string> Tags { get; set; }
        public Dimensions Dimensions { get; set; }
        public GeographicalLocation WarehouseLocation { get; set; }
    }

    public class Dimensions
    {
        public decimal Length { get; set; }
        public decimal Width { get; set; }
        public decimal Height { get; set; }
    }

    public class GeographicalLocation
    {
        public decimal Latitude { get; set; }
        public decimal Longitude { get; set; }
    }

    /// <remarks>
    ///     Internally, the JSON serializer uses Newtonsoft.Json for
    ///     serialization and NJsonSchema for schema creation and
    ///     validation.
    /// </remarks>
    class Program
    {
        public static Func<string, Product> Convertor = (json) =>{
            JObject obj = JObject.Parse(json);
            int productId = (int) obj["productId"];
            string productName = (string) obj["productName"];
            decimal latitude = (decimal) obj["warehouseLocation"]["latitude"];
            decimal longitude = (decimal) obj["warehouseLocation"]["longitude"];
            GeographicalLocation geographicalLocation = new GeographicalLocation{
                Latitude = latitude,
                Longitude = longitude
            };
            Product product = new Product{
                ProductId = productId,
                ProductName = productName,
                WarehouseLocation = geographicalLocation
            };
            return product;
        };
        // from: https://json-schema.org/learn/getting-started-step-by-step.html
        private static string S1;
        private static string S2;
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

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "json-example-consumer-group"
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };

            var srInitial = new CachedSchemaRegistryClient(schemaRegistryConfig);
            var sr = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var subject1 = $"{topicName}-CoordinatesOnMap";
            var subject2 = $"{topicName}-Product";

            // Test there are no errors (exceptions) registering a schema that references another.
            var id1 = srInitial.RegisterSchemaAsync(subject1, new Schema(S1, Confluent.SchemaRegistry.SchemaType.Json)).Result;
            var s1 = srInitial.GetLatestSchemaAsync(subject1).Result;
            var refs = new List<SchemaReference> { new SchemaReference("geographical-location.json", subject1, s1.Version) };
            var id2 = srInitial.RegisterSchemaAsync(subject2, new Schema(S2, refs, Confluent.SchemaRegistry.SchemaType.Json)).Result;

            // In fact, it seems references are not checked server side.
            var latestSchema2 = sr.GetLatestSchemaAsync(subject2).Result;
            var latestSchema2_unreg = latestSchema2.Schema;
            var latestSchema1 = sr.GetLatestSchemaAsync(subject1).Result;

            var jsonSerializerConfig = new JsonSerializerConfig
            {
                BufferBytes = 100,
                UseLatestVersion = true,
                AutoRegisterSchemas = false,
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };

            CancellationTokenSource cts = new CancellationTokenSource();

            var consumeTask = Task.Run(() =>
            {
                using (var consumer =
                    new ConsumerBuilder<string, Product>(consumerConfig)
                        .SetKeyDeserializer(Deserializers.Utf8)
                        // uncomment this and comment the line below to pass Convertor to deserializer
                        // .SetValueDeserializer(new JsonDeserializer<Product>(sr, latestSchema2_unreg, Convertor: Convertor).AsSyncOverAsync())
                        .SetValueDeserializer(new JsonDeserializer<Product>(sr, latestSchema2_unreg).AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    consumer.Subscribe(topicName);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var cr = consumer.Consume(cts.Token);
                                var product = cr.Message.Value;

                                System.Console.WriteLine("CONSUMER: product name " + product.ProductName +
                                    " Product id " + product.ProductId.ToString() + " latitude " +
                                    product.WarehouseLocation.Latitude.ToString());
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, Object>(producerConfig)
                    .SetValueSerializer(new NonGenericJsonSerializer(schemaRegistry, latestSchema2_unreg, jsonSerializerConfig))
                    // .SetValueSerializer(new JsonSerializer<Object>(schemaRegistry, latestSchema2_unreg, jsonSerializerConfig))
                    .Build())
            {
                Console.WriteLine($"PRODUCER: {producer.Name} producing on {topicName}. Enter product name, q to exit.");

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
                    Console.WriteLine($"{producer.Name} producing on {topicName}. Enter product name, q to exit.");
                }
            }
            cts.Cancel();
        }
    }
}
