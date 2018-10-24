﻿// Copyright 2018 Confluent Inc.
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
using Confluent.Kafka.AvroSerdes;
using Confluent.SchemaRegistry;
using Avro;
using Avro.Generic;


namespace Confluent.Kafka.Examples.AvroGeneric
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

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            string groupName = "avro-generic-example-group";

            // var s = (RecordSchema)Schema.Parse(File.ReadAllText("my-schema.json"));
            var s = (RecordSchema)Avro.Schema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(async () =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl }))
                using (var consumer = new Consumer(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupName }))
                {
                    var keyDeserializer = new AvroDeserializer<string>(schemaRegistry);
                    var valueDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry);

                    consumer.OnError += (_, e)
                        => Console.WriteLine($"Error: {e.Reason}");

                    consumer.Subscribe(topicName);

                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = await consumer.ConsumeAsync(keyDeserializer, valueDeserializer, cts.Token);

                            Console.WriteLine($"Key: {consumeResult.Message.Key}\nValue: {consumeResult.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine("Consume error: " + e.Error.Reason);
                        }
                    }

                    consumer.Close();
                }
            }, cts.Token);

            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl }))
            using (var producer = new Producer(new ProducerConfig { BootstrapServers = bootstrapServers }))
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

                var keySerializer = new AvroSerializer<string>(schemaRegistry);
                var valueSerializer = new AvroSerializer<GenericRecord>(schemaRegistry);

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var record = new GenericRecord(s);
                    record.Add("name", text);
                    record.Add("favorite_number", i++);
                    record.Add("favorite_color", "blue");

                    await producer
                        .ProduceAsync(keySerializer, valueSerializer, topicName, 
                            new Message<string, GenericRecord> { Key = text, Value = record })
                        .ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}");
                }
            }

            cts.Cancel();
        }
    }
}
