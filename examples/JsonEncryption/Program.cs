﻿// Copyright 2024 Confluent Inc.
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
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Confluent.SchemaRegistry.Encryption.Azure;
using Confluent.SchemaRegistry.Encryption.Gcp;
using Confluent.SchemaRegistry.Encryption.HcVault;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;


/// <summary>
///     An example of working with JSON data, Apache Kafka and 
///     Confluent Schema Registry (v5.5 or later required for
///     JSON schema support).
/// </summary>
namespace Confluent.Kafka.Examples.JsonSerialization
{
    /// <summary>
    ///     A POCO class corresponding to the JSON data written
    ///     to Kafka, where the schema is implicitly defined through 
    ///     the class properties and their attributes.
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
    class User
    {
        [JsonRequired] // use Newtonsoft.Json annotations
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonRequired]
        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }

        [JsonProperty("favorite_number")]
        public long FavoriteNumber { get; set; }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 6)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName kekName kmsType kmsKeyId");
                return;
            }

            // Register the KMS drivers and the field encryption executor
            AwsKmsDriver.Register();
            AzureKmsDriver.Register();
            GcpKmsDriver.Register();
            HcVaultKmsDriver.Register();
            FieldEncryptionExecutor.Register();

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            string kekName = args[3];
            string kmsType = args[4]; // one of aws-kms, azure-kms, gcp-kms, hcvault
            string kmsKeyId = args[5];
            string subjectName = topicName + "-value";

            var schemaStr = @"{
              ""type"": ""object"",
              ""properties"": {
                ""FavoriteColor"": {
                  ""type"": ""string""
                },
                ""FavoriteNumber"": {
                  ""type"": ""number""
                },
                ""Name"": {
                  ""type"": ""string"",
                  ""confluent:tags"": [ ""PII"" ]
                }
              }
            }";
            
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                // Note: you can specify more than one schema registry url using the
                // schema.registry.url property for redundancy (comma separated list). 
                // The property name is not plural to follow the convention set by
                // the Java implementation.
                Url = schemaRegistryUrl
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "json-example-consumer-group"
            };

            // Note: Specifying json serializer configuration is optional.
            var jsonSerializerConfig = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                BufferBytes = 100
            };

            RuleSet ruleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("encryptPII", RuleKind.Transform, RuleMode.WriteRead, "ENCRYPT", new HashSet<string>
                    {
                        "PII"
                    }, new Dictionary<string, string>
                    {
                        ["encrypt.kek.name"] = kekName,
                        ["encrypt.kms.type"] = kmsType,
                        ["encrypt.kms.key.id"] = kmsKeyId,
                    }, null, null, "ERROR,NONE", false)
                }
            );
            Schema schema = new Schema(schemaStr, null, SchemaType.Json, null, ruleSet);

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string, User>(consumerConfig)
                        .SetKeyDeserializer(Deserializers.Utf8)
                        .SetValueDeserializer(new JsonDeserializer<User>(schemaRegistry).AsSyncOverAsync())
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
                                var user = cr.Message.Value;
                                Console.WriteLine($"user name: {user.Name}, favorite number: {user.FavoriteNumber}, favorite color: {user.FavoriteColor}");
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
                new ProducerBuilder<string, User>(producerConfig)
                    .SetValueSerializer(new JsonSerializer<User>(schemaRegistry, jsonSerializerConfig))
                    .Build())
            {
                await schemaRegistry.RegisterSchemaAsync(subjectName, schema, true);
                    
                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter first names, q to exit.");

                long i = 1;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    User user = new User { Name = text, FavoriteColor = "blue", FavoriteNumber = i++ };
                    try 
                    {
                        await producer.ProduceAsync(topicName, new Message<string, User> { Value = user });
                    }
                    catch (Exception e) 
                    {
                        Console.WriteLine($"error producing message: {e.Message}");
                    }
                }
            }

            cts.Cancel();

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                // Note: a subject name strategy was not configured, so the default "Topic" was used.
                schema = await schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName));
                Console.WriteLine("\nThe JSON schema corresponding to the written data:");
                Console.WriteLine(schema.SchemaString);
            }
        }
    }
}
