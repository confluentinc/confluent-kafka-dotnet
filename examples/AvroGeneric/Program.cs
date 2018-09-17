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

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var avroConfig = new AvroSerdeProviderConfig
            {
                // Note: you can specify more than one schema registry url using the
                // schemaRegistryUrl property for redundancy (comma separated list).
                SchemaRegistryUrl = schemaRegistryUrl,
                // optional schema registry client properties:
                // SchemaRegistryRequestTimeoutMs = 5000, 
                // SchemaRegistryMaxCachedSchemas = 10,
                // optional avro serializer properties:
                // AvroSerializerBufferBytes = 50,
                // AvroSerializerAutoRegisterSchemas = true
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
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
                using (var serdeProvider = new AvroSerdeProvider(avroConfig))
                using (var consumer = new Consumer<string, GenericRecord>(consumerConfig, serdeProvider.DeserializerGenerator<string>(), serdeProvider.DeserializerGenerator<GenericRecord>()))
                {
                    consumer.OnError += (_, e)
                        => Console.WriteLine($"Error: {e.Reason}");

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

            using (var serdeProvider = new AvroSerdeProvider(avroConfig))
            using (var producer = new Producer<string, GenericRecord>(producerConfig, serdeProvider.SerializerGenerator<string>(), serdeProvider.SerializerGenerator<GenericRecord>()))
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
