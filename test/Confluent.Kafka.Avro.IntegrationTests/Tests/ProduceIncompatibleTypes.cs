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
using System.Threading;
using Confluent.Kafka.AvroSerdes;
using Confluent.SchemaRegistry;
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that producing messages with a key or value with incompatible schema
        ///     throws a SerializationException.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceIncompatibleTypes(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetResetType.Earliest,
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = schemaRegistryServers
            };

            var topic = Guid.NewGuid().ToString();
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {
                producer
                    .ProduceAsync(
                        topic, new Message<string, string> { Key = "hello", Value = "world" },
                        SerdeType.Avro, SerdeType.Avro)
                    .Wait();

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {
                Assert.Throws<SerializationException>(() =>
                {
                    try
                    {
                        producer
                            .ProduceAsync(
                                topic, new Message<int, string> { Key = 42, Value = "world" },
                                SerdeType.Avro, SerdeType.Avro)
                            .Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new AvroProducer(schemaRegistry, producerConfig))
            {                
                Assert.Throws<SerializationException>(() =>
                {
                    try
                    {
                        producer
                            .ProduceAsync(
                                topic, new Message<string, int> { Key = "world", Value = 42 },
                                SerdeType.Avro, SerdeType.Avro)
                            .Wait();
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
