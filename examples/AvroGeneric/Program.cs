// Copyright 2018 Confluent Inc.
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
using Confluent.Kafka.Serialization;
using Avro;
using Avro.Generic;


namespace Confluent.Kafka.Examples.AvroSpecific
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                // Note: you can specify more than one schema registry url using the
                // schema.registry.url property for redundancy (comma separated list).
                // The property name is not plural to follow the convention set by
                // the Java implementation.
                { "schema.registry.url", schemaRegistryUrl },
                // optional schema registry client properties:
                // { "schema.registry.connection.timeout.ms", 5000 },
                // { "schema.registry.max.cached.schemas", 10 },
                // optional avro serializer properties:
                // { "avro.serializer.buffer.bytes", 50 },
                // { "avro.serializer.auto.register.schemas", true }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid() },
                { "schema.registry.url", schemaRegistryUrl },
                { "error_cb", (Action<ErrorEvent>)(e => Console.WriteLine($"Error [{e.Level}]: {e.Error.Reason}")) }
            };

            // var s = (RecordSchema)Schema.Parse(File.ReadAllText("my-schema.json"));
            var s = (RecordSchema)Schema.Parse(
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
            var consumeTask = Task.Run(() =>
            {
                using (var consumer = new Consumer<string, GenericRecord>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<GenericRecord>()))
                {
                    consumer.Subscribe(topicName);

                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
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

            using (var producer = new Producer<string, GenericRecord>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<GenericRecord>()))
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}. Enter user names, q to exit.");

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var record = new GenericRecord(s);
                    record.Add("name", text);
                    record.Add("favorite_number", i++);
                    record.Add("favorite_color", "blue");

                    producer
                        .ProduceAsync(topicName, new Message<string, GenericRecord> { Key = text, Value = record })
                        .ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}");
                }
            }

            cts.Cancel();
        }
    }
}
