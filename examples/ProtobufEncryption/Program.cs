// Copyright 2018-2024 Confluent Inc.
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
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Confluent.SchemaRegistry.Encryption.Azure;
using Confluent.SchemaRegistry.Encryption.Gcp;
using Confluent.SchemaRegistry.Encryption.HcVault;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


/// <summary>
///     An example of working with protobuf serialized data and
///     Confluent Schema Registry (v5.5 or later required for
///     Protobuf schema support).
/// </summary>
namespace Confluent.Kafka.Examples.Protobuf
{
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

            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            message User {
                string Name = 1 [(.confluent.field_meta) = { tags: ""PII"" }];
                int64 FavoriteNumber = 2;
                string FavoriteColor = 3;
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
                Url = schemaRegistryUrl,
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "protobuf-example-consumer-group"
            };

            var protobufSerializerConfig = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                // optional Avro serializer properties:
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
            Schema schema = new Schema(schemaStr, null, SchemaType.Protobuf, null, ruleSet);

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string, User>(consumerConfig)
                        .SetValueDeserializer(new ProtobufDeserializer<User>(schemaRegistry).AsSyncOverAsync())
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
                                var consumeResult = consumer.Consume(cts.Token);
                                var user = consumeResult.Message.Value;
                                Console.WriteLine($"key: {consumeResult.Message.Key} user name: {user.Name}, favorite number: {user.FavoriteNumber}, favorite color: {user.FavoriteColor}");
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
                    .SetValueSerializer(new ProtobufSerializer<User>(schemaRegistry, protobufSerializerConfig))
                    .Build())
            {
                await schemaRegistry.RegisterSchemaAsync(subjectName, schema, true);
                    
                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

                long i = 1;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    User user = new User { Name = text, FavoriteColor = "green", FavoriteNumber = i++ };
                    await producer
                        .ProduceAsync(topicName, new Message<string, User> { Key = text, Value = user })
                        .ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}");
                }
            }

            cts.Cancel();
        }
    }
}
