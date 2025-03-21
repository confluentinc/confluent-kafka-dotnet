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

using Avro;
using Avro.Generic;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using System;
using System.Threading;
using System.Threading.Tasks;
using Schema = Avro.Schema;


namespace Confluent.Kafka.Examples.AvroGeneric
{
    class Program
    {
        static async Task Main(string[] args)
        {
            args = new string[3];
            args[0] =
                "lynqs-kafka-broker-01.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-02.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-03.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-04.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-05.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-06.lynqs.dev.gcp.fpprod.corp:9092";
            args[1] = "http://lynqs-kafka-schema-registry.lynqs.dev.gcp.fpprod.corp:80";
            args[2] = "DEV_HZNSAT_PUBLIC_BxEodTrades_1";

            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

            string groupName = "avro-generic-example-group1";

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];
            // var s = (RecordSchema)RecordSchema.Parse(File.ReadAllText("my-schema.json"));
            var s = (RecordSchema)Schema.Parse(@"{
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": ""long""},
                        {""name"": ""favorite_color"", ""type"": ""string""}
                    ]
                  }");

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                //using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
                using (var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
                           {
                               BootstrapServers = bootstrapServers,
                               GroupId = groupName,
                               SaslUsername = "sophis-sophis-jna",
                               SaslPassword = "vq8NKGA84Zadd",
                               SaslMechanism = SaslMechanism.Plain,
                               SecurityProtocol = SecurityProtocol.SaslSsl,
                           }).
                           SetValueDeserializer(
                    Deserializers.Utf8
                    //new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync()
                    )
                           .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}")).Build())
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
                        consumer.Close();
                    }
                }
            });


            Console.ReadLine();
            cts.Cancel();
        }
    }
}