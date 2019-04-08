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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka;
using Google.Protobuf;
using System;
using System.Threading;
using System.Threading.Tasks;


/// <summary>
///     A simple example demonstrating how to produce and consume protobuf serialized data.
///     Note: Does not demonstrate integration with Schema Registry which currently only supports Avro.
/// </summary>
namespace Confluent.Kafka.Examples.Protobuf
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = args[0],
                GroupId = "protobuf-example",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            var messageParser = new MessageParser<User>(() => new User());

            var consumeTask = Task.Run(() =>
            {
                // consume a single message then exit.
                using (var consumer =
                    new ConsumerBuilder<int, User>(consumerConfig)
                        .SetValueDeserializer((data, isNull) => messageParser.ParseFrom(data.ToArray()))
                        .Build())
                {
                    consumer.Subscribe("protobuf-test-topic");
                    var cr = consumer.Consume();
                    Console.WriteLine($"User: [id: {cr.Key}, favorite color: {cr.Message.Value.FavoriteColor}]");
                }
            });

            // wait a bit so the consumer is ready to consume messages before producing one.
            await Task.Delay(TimeSpan.FromSeconds(10));

            var producerConfig = new ProducerConfig { BootstrapServers = args[0] };

            using (var producer =
                new ProducerBuilder<int, User>(producerConfig)
                    .SetValueSerializer(data => data.ToByteArray())
                    .Build())
            {
                await producer.ProduceAsync("protobuf-test-topic", new Message<int, User> { Key = 0, Value = new User { FavoriteColor = "green" } });
            }

            await consumeTask;
        }
    }
}
