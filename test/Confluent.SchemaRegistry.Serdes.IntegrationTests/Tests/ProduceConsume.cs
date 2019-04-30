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
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceConsume(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = schemaRegistryServers
            };

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };

            string topic = Guid.NewGuid().ToString();
            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, User>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                    .SetValueSerializer(new AvroSerializer<User>(schemaRegistry))
                    .Build())
            {
                for (int i = 0; i < 100; ++i)
                {
                    var user = new User
                    {
                        name = i.ToString(),
                        favorite_number = i,
                        favorite_color = "blue"
                    };
                    
                    producer
                        .ProduceAsync(topic, new Message<string, User> { Key = user.name, Value = user })
                        .Wait();
                }
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, User>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Assert.True(false, e.Reason))
                    .Build())
            {
                consumer.Subscribe(topic);

                int i = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }

                    Assert.Equal(i.ToString(), record.Message.Key);
                    Assert.Equal(i.ToString(), record.Message.Value.name);
                    Assert.Equal(i, record.Message.Value.favorite_number);
                    Assert.Equal("blue", record.Message.Value.favorite_color);
                    i += 1;
                }

                Assert.Equal(100, i);

                consumer.Close();
            }
        }

    }
}
