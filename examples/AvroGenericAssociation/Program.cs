// Copyright 2025 Confluent Inc.
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
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Schema = Confluent.SchemaRegistry.Schema;


namespace Confluent.Kafka.Examples.AvroGenericAssociation
{
    /// <summary>
    ///     Demonstrates how to use the Associated subject name strategy with
    ///     Avro GenericRecord serialization. This example:
    ///     1. Registers a schema under a custom subject name.
    ///     2. Creates a STRONG association between the topic and the subject.
    ///     3. Produces and consumes messages using the Associated strategy
    ///        (which looks up the subject via the association).
    ///     4. Deletes the association.
    /// </summary>
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            string groupName = "avro-generic-association-example-group";
            string subjectName = $"{topicName}-custom-value";

            var s = (RecordSchema)RecordSchema.Parse(
                @"{
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": ""long""},
                        {""name"": ""favorite_color"", ""type"": ""string""}
                    ]
                  }"
            );

            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
            {
                // Step 1: Register the schema under a custom subject name.
                var schemaObj = new Schema(s.ToString(), new List<SchemaReference>(), SchemaType.Avro);
                var registeredSchema = await schemaRegistry.RegisterSchemaWithResponseAsync(subjectName, schemaObj, false);
                Console.WriteLine($"Registered schema under subject '{subjectName}' with id {registeredSchema.Id}");

                // Step 2: Create a STRONG association between the topic and the subject.
                var associationRequest = new AssociationCreateOrUpdateRequest(
                    resourceName: topicName,
                    resourceNamespace: "lkc-123",
                    resourceId: "lkc-123:" + topicName,
                    resourceType: "topic",
                    associations: new List<AssociationCreateOrUpdateInfo>
                    {
                        new AssociationCreateOrUpdateInfo(
                            subject: subjectName,
                            associationType: "value",
                            lifecycle: "STRONG",
                            frozen: null,
                            schema: null,
                            normalize: null
                        )
                    }
                );
                var associationResponse = await schemaRegistry.CreateAssociationAsync(associationRequest);
                Console.WriteLine($"Created STRONG association: topic '{topicName}' -> subject '{subjectName}'");

                // Step 3: Produce and consume using the Associated strategy (the default).
                CancellationTokenSource cts = new CancellationTokenSource();
                var consumeTask = Task.Run(() =>
                {
                    using (var consumer =
                        new ConsumerBuilder<string, GenericRecord>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupName, AutoOffsetReset = AutoOffsetReset.Earliest })
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
                                    Console.WriteLine($"Consumed - Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}");
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

                using (var producer =
                    new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = bootstrapServers })
                        .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, new AvroSerializerConfig
                        {
                            AutoRegisterSchemas = false,
                            UseLatestVersion = true
                        }))
                        .Build())
                {
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
                            Console.WriteLine($"Produced to: {dr.TopicPartitionOffset}");
                        }
                        catch (ProduceException<string, GenericRecord> ex)
                        {
                            Console.WriteLine($"Error producing message: {ex}");
                        }
                    }
                }

                cts.Cancel();
                await consumeTask;

                // Step 4: Delete the association.
                var associations = await schemaRegistry.GetAssociationsByResourceNameAsync(
                    topicName, "-", "topic", null, null, 0, -1);
                if (associations.Count > 0)
                {
                    await schemaRegistry.DeleteAssociationsAsync(
                        associations[0].ResourceId, "topic", new List<string> { "value" }, true);
                    Console.WriteLine($"Deleted associations for topic '{topicName}'");
                }
            }
        }
    }
}
