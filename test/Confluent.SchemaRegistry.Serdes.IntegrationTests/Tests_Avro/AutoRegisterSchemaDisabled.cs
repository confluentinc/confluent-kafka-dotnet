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
using Confluent.Kafka;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test behavior when avro.serializer.auto.register.schemas == false.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void AutoRegisterSchemaDisabled(string bootstrapServers, string schemaRegistryServers)
        {
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
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

                // first a quick check the value case fails.

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                    new ProducerBuilder<string, int>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                        .SetValueSerializer(new AvroSerializer<int>(schemaRegistry, new AvroSerializerConfig { AutoRegisterSchemas = false }))
                        .Build())
                {
                    Assert.Throws<SchemaRegistryException>(() =>
                    {
                        string guidTopic = Guid.NewGuid().ToString();
                        try
                        {
                            producer
                                .ProduceAsync(guidTopic, new Message<string, int> { Key = "test", Value = 112 })
                                .GetAwaiter()
                                .GetResult();
                        }
                        catch (Exception e)
                        {
                            Assert.True(e is ProduceException<string, int>);
                            Assert.Equal(ErrorCode.Local_ValueSerialization, ((ProduceException<string, int>)e).Error.Code);

                            // Test message fields are appropriately set in the case of a serialization error.
                            Assert.Equal("test", ((ProduceException<string, int>)e).DeliveryResult.Key);
                            Assert.Equal(112, ((ProduceException<string, int>)e).DeliveryResult.Value);
                            Assert.Equal(Offset.Unset, ((ProduceException<string, int>)e).DeliveryResult.Offset);
                            Assert.Equal(Partition.Any, ((ProduceException<string, int>)e).DeliveryResult.Partition);
                            Assert.Equal(guidTopic, ((ProduceException<string, int>)e).DeliveryResult.Topic);
                            Assert.Equal(PersistenceStatus.NotPersisted, ((ProduceException<string, int>)e).DeliveryResult.Status);
                            Assert.Equal(Timestamp.Default, ((ProduceException<string, int>)e).DeliveryResult.Timestamp);
                            Assert.Null(((ProduceException<string, int>)e).DeliveryResult.Headers);

                            // should be SerializationException.
                            throw e.InnerException;
                        }
                    });
                }

                // the following tests all check behavior in the key case.

                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                    new ProducerBuilder<string, int>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, new AvroSerializerConfig { AutoRegisterSchemas = false }))
                        .SetValueSerializer(new AvroSerializer<int>(schemaRegistry))
                        .Build())
                {
                    Assert.Throws<SchemaRegistryException>(() =>
                    {
                        try
                        {
                            producer.ProduceAsync(topic.Name, new Message<string, int> { Key = "test", Value = 112 })
                                .GetAwaiter()
                                .GetResult();
                        }
                        catch (Exception e)
                        {
                            Assert.True(e is ProduceException<string, int>);
                            throw e.InnerException;
                        }
                    });
                }

                // allow auto register..
                using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
                using (var producer =
                    new ProducerBuilder<string, int>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                        .SetValueSerializer(new AvroSerializer<int>(schemaRegistry))
                        .Build())
                {
                    producer.ProduceAsync(topic.Name, new Message<string, int> { Key = "test", Value = 112 }).Wait();
                }

                // config with avro.serializer.auto.register.schemas == false should work now.
                using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryServers }))
                using (var producer =
                    new ProducerBuilder<string, int>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, new AvroSerializerConfig { AutoRegisterSchemas = false }))
                        .SetValueSerializer(new AvroSerializer<int>(schemaRegistry))
                        .Build())
                {
                    producer.ProduceAsync(topic.Name, new Message<string, int> { Key = "test", Value = 112 }).Wait();
                }

                // config with avro.serializer.use.latest.version == true should also work now.
                using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryServers }))
                using (var producer =
                    new ProducerBuilder<string, int>(producerConfig)
                        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true}))
                        .SetValueSerializer(new AvroSerializer<int>(schemaRegistry))
                        .Build())
                {
                    producer.ProduceAsync(topic.Name, new Message<string, int> { Key = "test", Value = 112 }).Wait();
                }
            }
        }
    }
}
