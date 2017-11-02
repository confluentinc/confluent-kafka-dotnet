// Copyright 2016-2017 Confluent Inc.
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
using Confluent.Kafka.SchemaRegistry;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka.Examples.AvroSpecific
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage:   AvroSpecific  bootstrapServers schemaregistryurl topicName");
                return;
            }

            string bootstrapServers = args[0];
            string schemaRegistryUrl = args[1];
            string topicName = args[2];

            var schemaRegistryConfig = new Dictionary<string, object>{ { "schema.registry.urls", schemaRegistryUrl } };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };
            var consumerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers }, { "group.id", Guid.NewGuid() } };

            using (var schemaRegistry = new SchemaRegistryClient(schemaRegistryConfig))
            using (var consumer = new Consumer<User, User>(consumerConfig, new AvroDeserializer<User>(schemaRegistry), new AvroDeserializer<User>(schemaRegistry)))
            using (var producer = new Producer<User, User>(producerConfig, new AvroSerializer<User>(schemaRegistry), new AvroSerializer<User>(schemaRegistry)))
            {
                consumer.OnMessage += (o, e) =>
                {
                    var record = e.Value;
                    Console.WriteLine($"user key name: {e.Key.name}, user value favorite color: {e.Value.favorite_color}");
                };
                
                consumer.OnError += (_, e)
                    => Console.WriteLine(e.Reason);

                consumer.OnConsumeError += (_, e)
                    => Console.WriteLine(e.Error.Reason);
                
                consumer.Subscribe(topicName);
                CancellationTokenSource cts = new CancellationTokenSource();
                var consumeTask = Task.Factory.StartNew(() =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        consumer.Poll(100);
                    }
                });

                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    User user = new User { name = text, favorite_color = "green", favorite_number = i++ };
                    var deliveryReport = producer.ProduceAsync(topicName, user, user).Result;
                    Console.WriteLine($"Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
                }

                cts.Cancel();
                consumeTask.Wait();
            }
        }
    }
}
