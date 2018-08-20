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
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.Serialization;
using Confluent.SchemaRegistry;
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test the variants of AvroSerializer and AvroDeserializer that accept 
        ///     a pre-constructed ISchemaRegistry instance work.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void SharedSchemaRegistryClient(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid().ToString() },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" },
                { "error_cb", (Action<Error>)(error => Assert.True(false, error.Reason)) }
            };

            var schemaRegistryConfig = new Dictionary<string, object>
            {
                { "schema.registry.url", schemaRegistryServers }
            };

            using (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                using (var producer = new Producer<string, User>(producerConfig, new AvroSerializer<string>(schemaRegistryClient), new AvroSerializer<User>(schemaRegistryClient)))
                {
                    for (int i = 0; i < 100; ++i)
                    {
                        var user = new User
                        {
                            name = i.ToString(),
                            favorite_number = i,
                            favorite_color = "blue"
                        };
                        producer.ProduceAsync(topic, new Message<string, User> { Key = user.name, Value = user });
                    }
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var consumer = new Consumer<string, User>(consumerConfig, new AvroDeserializer<string>(schemaRegistryClient), new AvroDeserializer<User>(schemaRegistryClient)))
                {
                    consumer.Subscribe(topic);

                    int i = 0;

                    while (true)
                    {
                        var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (record.Message != null)
                        {
                            Assert.Equal(i.ToString(), record.Message.Key);
                            Assert.Equal(i.ToString(), record.Message.Value.name);
                            Assert.Equal(i, record.Message.Value.favorite_number);
                            Assert.Equal("blue", record.Message.Value.favorite_color);
                            i += 1;
                        }
                        if (record.IsPartitionEOF)
                        {
                            break;
                        }
                    }

                    Assert.Equal(100, i);

                    consumer.Close();
                }
            }
        }
    }
}
