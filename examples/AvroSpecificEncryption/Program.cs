// Copyright 2024 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Confluent.SchemaRegistry.Encryption.Azure;
using Confluent.SchemaRegistry.Encryption.Gcp;
using Confluent.SchemaRegistry.Encryption.HcVault;
using Confluent.SchemaRegistry.Serdes;


namespace Confluent.Kafka.Examples.AvroSpecificEncryption
{
    class Program
    {
        static void Main(string[] args)
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
            LocalKmsDriver.Register();
            FieldEncryptionExecutor.Register();

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            string kekName = args[3];
            string kmsType = args[4]; // one of aws-kms, azure-kms, gcp-kms, hcvault
            string kmsKeyId = args[5];
            string subjectName = topicName + "-value";

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
                GroupId = "avro-specific-example-group"
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                // optional Avro serializer properties:
                BufferBytes = 100
            };
            // KMS properties can be passed as follows
            // avroSerializerConfig.Set("rules.secret.access.key", "xxx");
            // avroSerializerConfig.Set("rules.access.key.id", "xxx");

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
            Schema schema = new Schema(User._SCHEMA.ToString(), null, SchemaType.Avro, null, ruleSet);

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string, User>(consumerConfig)
                        .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
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
                                Console.WriteLine($"key: {consumeResult.Message.Key}, user name: {user.name}, favorite number: {user.favorite_number}, favorite color: {user.favorite_color}, hourly_rate: {user.hourly_rate}");
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
                    .SetValueSerializer(new AvroSerializer<User>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                schemaRegistry.RegisterSchemaAsync(subjectName, schema, true);

                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

                int i = 1;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    User user = new User { name = text, favorite_color = "green", favorite_number = ++i, hourly_rate = new Avro.AvroDecimal(67.99) };
                    producer
                        .ProduceAsync(topicName, new Message<string, User> { Key = text, Value = user })
                        .ContinueWith(task =>
                            {
                                if (!task.IsFaulted)
                                {
                                    Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                                    return;
                                }

                                // Task.Exception is of type AggregateException. Use the InnerException property
                                // to get the underlying ProduceException. In some cases (notably Schema Registry
                                // connectivity issues), the InnerException of the ProduceException will contain
                                // additional information pertaining to the root cause of the problem. Note: this
                                // information is automatically included in the output of the ToString() method of
                                // the ProduceException which is called implicitly in the below.
                                Console.WriteLine($"error producing message: {task.Exception.InnerException}");
                            });
                }
            }

            cts.Cancel();
        }
    }
}
