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

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using System;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test producing/consuming using both regular and Avro serializers.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void AvoAndRegular(string bootstrapServers, string schemaRegistryServers)
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
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = schemaRegistryServers
                };

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                    new ProducerBuilder<string, string>(producerConfig)
                        .SetKeySerializer(Serializers.Utf8)
                        .SetValueSerializer(new AvroSerializer<string>(schemaRegistry))
                        .Build())
                {
                    // implicit check that this does not fail.
                    producer.ProduceAsync(topic1.Name, new Message<string, string> { Key = "hello", Value = "world" }).Wait();

                    // check that the value type was registered with SR, and the key was not.
                    Assert.Throws<SchemaRegistryException>(() =>
                        {
                            try
                            {
                                schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructKeySubjectName(topic1.Name, null)).Wait();
                            }
                            catch (AggregateException e)
                            {
                                throw e.InnerException;
                            }
                        });
                    var s2 = schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructValueSubjectName(topic1.Name, null)).Result;
                }

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                    new ProducerBuilder<string, string>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                        .SetValueSerializer(Serializers.Utf8)
                        .Build())
                {
                    // implicit check that this does not fail.
                    producer.ProduceAsync(topic2.Name, new Message<string, string> { Key = "hello", Value = "world" }).Wait();

                    // check that the key type was registered with SR, and the value was not.
                    Assert.Throws<SchemaRegistryException>(() =>
                        {
                            try
                            {
                                schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructValueSubjectName(topic2.Name, null)).Wait();
                            }
                            catch (AggregateException e)
                            {
                                throw e.InnerException;
                            }
                        });
                    var s2 = schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructKeySubjectName(topic2.Name, null)).Result;
                }

                // check the above can be consumed (using regular / Avro serializers as appropriate)
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                {
                    using (var consumer =
                        new ConsumerBuilder<string, string>(consumerConfig)
                            .SetKeyDeserializer(Deserializers.Utf8)
                            .SetValueDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                            .Build())
                    {
                        consumer.Assign(new TopicPartitionOffset(topic1.Name, 0, 0));
                        var cr = consumer.Consume();
                        Assert.Equal("hello", cr.Message.Key);
                        Assert.Equal("world", cr.Message.Value);
                    }

                    using (var consumer =
                        new ConsumerBuilder<string, string>(consumerConfig)
                            .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                            .SetValueDeserializer(Deserializers.Utf8).Build())
                    {
                        consumer.Assign(new TopicPartitionOffset(topic2.Name, 0, 0));
                        var cr = consumer.Consume();
                        Assert.Equal("hello", cr.Message.Key);
                        Assert.Equal("world", cr.Message.Value);
                    }

                    using (var consumer =
                        new ConsumerBuilder<string, string>(consumerConfig)
                            .SetKeyDeserializer(Deserializers.Utf8)
                            .SetValueDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                            .Build())
                    {
                        consumer.Assign(new TopicPartitionOffset(topic2.Name, 0, 0));
                        Assert.ThrowsAny<ConsumeException>(() => 
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

                    using (var consumer =
                        new ConsumerBuilder<string, string>(consumerConfig)
                            .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                            .SetValueDeserializer(Deserializers.Utf8)
                            .Build())
                    {
                        consumer.Assign(new TopicPartitionOffset(topic1.Name, 0, 0));
                        Assert.ThrowsAny<ConsumeException>(() =>
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