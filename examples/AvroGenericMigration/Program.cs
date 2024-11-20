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
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Schema = Confluent.SchemaRegistry.Schema;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;


namespace Confluent.Kafka.Examples.AvroGenericMigration
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

            // Register the KMS drivers and the field encryption executor
            JsonataExecutor.Register();

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            string subjectName = topicName + "-value";
            string groupName = "avro-generic-example-group";

            var avroSerializerConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestWithMetadata = new Dictionary<string, string>()
                {
                    ["application.major.version"] = "1"
                },
                // optional Avro serializer properties:
                BufferBytes = 100
            };

            var avroDeserializerConfig = new AvroDeserializerConfig
            {
                UseLatestWithMetadata = new Dictionary<string, string>()
                {
                    ["application.major.version"] = "2"
                }
            };

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

            Confluent.SchemaRegistry.Metadata metadata = new Confluent.SchemaRegistry.Metadata(
                null,
                new Dictionary<string, string>
                {
                    ["application.major.version"] = "1",
                },
                null
            );
            Schema schema = new Schema(s.ToString(), null, SchemaType.Avro, metadata, null);

            var s2 = (RecordSchema)RecordSchema.Parse(
                @"{
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string"", ""confluent:tags"": [""PII""]},
                        {""name"": ""fave_num"",  ""type"": ""long""},
                        {""name"": ""favorite_color"", ""type"": ""string""}
                    ]
                  }"
            );

            Confluent.SchemaRegistry.Metadata metadata2 = new Confluent.SchemaRegistry.Metadata(
                null,
                new Dictionary<string, string>
                {
                    ["application.major.version"] = "2",
                },
                null
            );
            String expr = "$merge([$sift($, function($v, $k) {$k != 'favorite_number'}), {'fave_num': $.'favorite_number'}])";
            RuleSet ruleSet = new RuleSet(new List<Rule>
                {
                    new Rule("upgrade", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null, null,
                        expr, null, null, false)
                }, new List<Rule>()
            );
            Schema schema2 = new Schema(s2.ToString(), null, SchemaType.Avro, metadata2, ruleSet);

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
                using (var consumer =
                    new ConsumerBuilder<string, GenericRecord>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupName })
                        .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry, avroDeserializerConfig).AsSyncOverAsync())
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
                var c = schemaRegistry.UpdateCompatibilityAsync(Compatibility.None, null).Result;
                var id = schemaRegistry.RegisterSchemaAsync(subjectName, schema, true).Result;
                var id2 = schemaRegistry.RegisterSchemaAsync(subjectName, schema2, true).Result;

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
