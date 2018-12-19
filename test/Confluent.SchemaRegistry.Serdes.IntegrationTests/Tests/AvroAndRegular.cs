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
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Examples.AvroSpecific;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test producing/consuming using both regular and Avro serializers.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void RegularAndAvro(string bootstrapServers, string schemaRegistryServers)
        {
            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
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
                    AutoOffsetReset = AutoOffsetResetType.Earliest
                };

                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    SchemaRegistryUrl = schemaRegistryServers
                };

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer = new Producer<string, string>(producerConfig,
                    Serializers.Utf8, new AvroSerializer<string>(schemaRegistry)))
                {
                    // implicit check that this does not fail.
                    producer.ProduceAsync(topic1.Name, new Message<string, string> { Key = "hello", Value = "world" }).Wait();

                    // check that the value type was registered with SR, and the key was not.
                    Assert.Throws<SchemaRegistryException>(() =>
                        {
                            try
                            {
                                schemaRegistry.GetLatestSchemaAsync(schemaRegistry.ConstructKeySubjectName(topic1.Name)).Wait();
                            }
                            catch (AggregateException e)
                            {
                                throw e.InnerException;
                            }
                        });
                    var s2 = schemaRegistry.GetLatestSchemaAsync(schemaRegistry.ConstructValueSubjectName(topic1.Name)).Result;
                }

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer = new Producer<string, string>(producerConfig,
                    new AvroSerializer<string>(schemaRegistry), Serializers.Utf8))
                {
                    // implicit check that this does not fail.
                    producer.ProduceAsync(topic2.Name, new Message<string, string> { Key = "hello", Value = "world" }).Wait();

                    // check that the key type was registered with SR, and the value was not.
                    Assert.Throws<SchemaRegistryException>(() =>
                        {
                            try
                            {
                                schemaRegistry.GetLatestSchemaAsync(schemaRegistry.ConstructValueSubjectName(topic2.Name)).Wait();
                            }
                            catch (AggregateException e)
                            {
                                throw e.InnerException;
                            }
                        });
                    var s2 = schemaRegistry.GetLatestSchemaAsync(schemaRegistry.ConstructKeySubjectName(topic2.Name)).Result;
                }

                // check the above can be consumed (using regular / Avro serializers as appropriate)
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                {
                    using (var consumer = new Consumer<string, string>(consumerConfig,
                        Deserializers.Utf8, new AvroDeserializer<string>(schemaRegistry)))
                    {
                        consumer.Assign(new TopicPartitionOffset(topic1.Name, 0, 0));
                        var cr = consumer.Consume();
                        Assert.Equal("hello", cr.Key);
                        Assert.Equal("world", cr.Value);
                    }

                    using (var consumer = new Consumer<string, string>(consumerConfig,
                        new AvroDeserializer<string>(schemaRegistry), Deserializers.Utf8))
                    {
                        consumer.Assign(new TopicPartitionOffset(topic2.Name, 0, 0));
                        var cr = consumer.Consume();
                        Assert.Equal("hello", cr.Key);
                        Assert.Equal("world", cr.Value);
                    }

                    using (var consumer = new Consumer<string, string>(consumerConfig,
                        Deserializers.Utf8, new AvroDeserializer<string>(schemaRegistry)))
                    {
                        consumer.Assign(new TopicPartitionOffset(topic2.Name, 0, 0));
                        Assert.ThrowsAny<DeserializationException>(() => 
                            {
                                try
                                {
                                    consumer.Consume();
                                }
                                catch (AggregateException e)
                                {
                                    throw e.InnerException;
                                }
                            });
                    }

                    using (var consumer = new Consumer<string, string>(consumerConfig,
                        new AvroDeserializer<string>(schemaRegistry), Deserializers.Utf8))
                    {
                        consumer.Assign(new TopicPartitionOffset(topic1.Name, 0, 0));
                        Assert.ThrowsAny<DeserializationException>(() =>
                            {
                                try
                                {
                                    consumer.Consume();
                                }
                                catch (AggregateException e)
                                {
                                    throw e.InnerException;
                                }
                            });
                    }
                }
            }
        }
    }
}