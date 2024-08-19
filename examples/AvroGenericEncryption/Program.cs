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

using Avro;
using Avro.Generic;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Confluent.SchemaRegistry.Encryption.Azure;
using Confluent.SchemaRegistry.Encryption.Gcp;
using Confluent.SchemaRegistry.Encryption.HcVault;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Schema = Confluent.SchemaRegistry.Schema;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka.Examples.AvroGenericEncryption
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 6)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
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
            string groupName = "avro-generic-example-group";

            // var s = (RecordSchema)RecordSchema.Parse(File.ReadAllText("my-schema.json"));
            var s = (RecordSchema)RecordSchema.Parse(
                @"{
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string"", ""confluent:tags"": [""PII""]},
                        {""name"": ""favorite_number"",  ""type"": ""long""},
                        {""name"": ""favorite_color"", ""type"": ""string""}
                    ]
                  }"
            );

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
            Schema schema = new Schema(s.ToString(), null, SchemaType.Avro, null, ruleSet);

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
                using (var consumer =
                    new ConsumerBuilder<string, GenericRecord>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupName })
                        .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
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

                                Console.WriteLine($"Key: {consumeResult.Message.Key}\nValue: {consumeResult.Message.Value}");
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // commit final offsets and leave the group.
                        consumer.Close();
                    }
                }
            });

            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
            using (var producer =
                new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = bootstrapServers })
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                schemaRegistry.RegisterSchemaAsync(subjectName, schema, true);

                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

                long i = 1;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var record = new GenericRecord(s);
                    record.Add("name", text);
                    record.Add("favorite_number", i++);
                    record.Add("favorite_color", "blue");

                    try
                    {
                        var dr = await producer.ProduceAsync(topicName, new Message<string, GenericRecord> { Key = text, Value = record });
                        Console.WriteLine($"produced to: {dr.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, GenericRecord> ex)
                    {
                        // In some cases (notably Schema Registry connectivity issues), the InnerException
                        // of the ProduceException contains additional informatiom pertaining to the root
                        // cause of the problem. This information is automatically included in the output
                        // of the ToString() method of the ProduceException, called implicitly in the below.
                        Console.WriteLine($"error producing message: {ex}");
                    }
                }
            }

            cts.Cancel();
        }
    }
}
