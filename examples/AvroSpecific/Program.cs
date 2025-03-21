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
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using com.leonteq.horizonsat.avro.berntrade;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;


namespace Confluent.Kafka.Examples.AvroSpecific
{
    class Program
    {
        static void Main(string[] args)
        {

            args = new string[3];

            args[0] =
                "lynqs-kafka-broker-01.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-02.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-03.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-04.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-05.lynqs.dev.gcp.fpprod.corp:9092,lynqs-kafka-broker-06.lynqs.dev.gcp.fpprod.corp:9092";
            args[1] = "http://lynqs-kafka-schema-registry.lynqs.dev.gcp.fpprod.corp:80";
            args[2] = "DEV_HZNSAT_PUBLIC_BxEodTrades_1";

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];

            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. bootstrapServers schemaRegistryUrl topicName");
                return;
            }

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
                GroupId = "LEonteq.Test.SoborSaif" + Guid.NewGuid(),
                SaslUsername = "sophis-sophis-jna",
                SaslPassword = "vq8NKGA84Zadd",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<Ignore, BernTrade>(consumerConfig)
                        .SetValueDeserializer(new AvroDeserializer<BernTrade>(schemaRegistry).AsSyncOverAsync())
                        .SetErrorHandler(ErrorHandler)
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
                                Console.WriteLine($"result: {consumeResult.Message.Value}");
                                Console.WriteLine($"message: {user}");
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

        private static void ErrorHandler(IConsumer<Ignore, BernTrade> _, Error e)
        {
            Console.WriteLine($"Error: {e.Reason}");
        }
    }
}
