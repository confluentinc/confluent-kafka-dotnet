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
using Confluent.Kafka.AvroSerdes;
using Confluent.SchemaRegistry;
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test behavior when avro.serializer.auto.register.schemas == false.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void AutoRegisterSchemaDisabled(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();

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


            // first a quick check the value case fails.

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {
                producer.RegisterAvroSerializer(new AvroSerializer<int>(new AvroSerializerConfig { AutoRegisterSchemas = false }));

                Assert.Throws<SerializationException>(() =>
                {
                    try
                    {
                        producer
                            .ProduceAsync(new Guid().ToString(), new Message<string, int> { Key = "test", Value = 112 }, SerdeType.Avro, SerdeType.Avro)
                            .Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            // the following tests all check behavior in the key case.

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {
                producer.RegisterAvroSerializer(new AvroSerializer<string>(new AvroSerializerConfig { AutoRegisterSchemas = false }));

                Assert.Throws<SerializationException>(() =>
                {
                    try
                    {
                        producer
                            .ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }, SerdeType.Avro, SerdeType.Avro)
                            .Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            // allow auto register..
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new Producer(producerConfig))
            {
                producer.ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }).Wait();
            }

            // config with avro.serializer.auto.register.schemas == false should work now.
            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryServers }))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {
                producer.RegisterAvroSerializer(new AvroSerializer<string>(new AvroSerializerConfig { AutoRegisterSchemas = false }));

                producer.ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }).Wait();
            }
        }
    }
}
