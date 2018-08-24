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
using System.IO;
using System.Linq;
using System.Threading;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace AvroBlogExample
{
    /// <summary>
    ///     Complete source for the examples programs presented in the blog post:
    ///     [insert blog URL here]
    /// </summary>
    class Program
    {
        static void ProduceGeneric(string bootstrapServers, string schemaRegistryUrl)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryUrl }
            };

            using (var producer = new Producer<Null, GenericRecord>(config, null, new AvroSerializer<GenericRecord>()))
            {
                var logLevelSchema = (EnumSchema)Schema.Parse(
                    File.ReadAllText("LogLevel.asvc"));

                var logMessageSchema = (RecordSchema)Schema
                    .Parse(File.ReadAllText("LogMessage.V1.asvc")
                        .Replace(
                            "MessageTypes.LogLevel", 
                            File.ReadAllText("LogLevel.asvc")));

                var record = new GenericRecord(logMessageSchema);
                record.Add("IP", "127.0.0.1");
                record.Add("Message", "a test log message");
                record.Add("Severity", new GenericEnum(logLevelSchema, "Error"));
                producer.ProduceAsync("log-messages", new Message<Null, GenericRecord> { Value = record })
                    .ContinueWith(task => Console.WriteLine(
                        task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}"));

                producer.Flush(TimeSpan.FromSeconds(30));
            }
        }

        static void ProduceSpecific(string bootstrapServers, string schemaRegistryUrl)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryUrl }
            };

            using (var producer = new Producer<Null, MessageTypes.LogMessage>(config, null, new AvroSerializer<MessageTypes.LogMessage>()))
            {
                producer.ProduceAsync("log-messages", 
                    new Message<Null, MessageTypes.LogMessage> 
                    {
                        Value = new MessageTypes.LogMessage
                        {
                            IP = "192.168.0.1",
                            Message = "a test message 2",
                            Severity = MessageTypes.LogLevel.Info,
                            Tags = new Dictionary<string, string> { { "location", "CA" } }
                        }
                    });
                producer.Flush(TimeSpan.FromSeconds(30));
            }
        }

        static void ConsumeSpecific(string bootstrapServers, string schemaRegistryUrl)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryUrl },
                { "auto.offset.reset", "beginning" }
            };

            using (var consumer = new Consumer<Null, MessageTypes.LogMessage>(
                consumerConfig, null, new AvroDeserializer<MessageTypes.LogMessage>()))
            {
                consumer.Subscribe("log-messages");

                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        Console.WriteLine($"{consumeResult.Message.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}: [{consumeResult.Value.Severity}] {consumeResult.Value.Message}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"an error occured: {e.Error.Reason}");
                        break;
                    }
                }

                consumer.Close();
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <generic-produce|specific-produce|consume> <bootstrap-servers> <schema-registry-url>");

        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                PrintUsage();
                return;
            }

            var mode = args[0];
            var bootstrapServers = args[1];
            var schemaRegistryUrl = args[2];

            switch (mode)
            {
                case "generic-produce":
                    ProduceGeneric(bootstrapServers, schemaRegistryUrl);
                    break;
                case "specific-produce":
                    ProduceSpecific(bootstrapServers, schemaRegistryUrl);
                    break;
                case "consume":
                    ConsumeSpecific(bootstrapServers, schemaRegistryUrl);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
