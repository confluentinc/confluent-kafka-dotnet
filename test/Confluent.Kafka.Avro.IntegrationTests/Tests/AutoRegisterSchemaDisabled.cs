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
using Confluent.Kafka.Serialization;
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

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var avroConfig = new AvroSerdeProviderConfig { SchemaRegistryUrl = schemaRegistryServers, AvroSerializerAutoRegisterSchemas = false };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var serdeProvider = new AvroSerdeProvider(avroConfig))
            using (var producer = new Producer<string, int>(producerConfig, serdeProvider.CreateKeySerializer<string>(), serdeProvider.CreateValueSerializer<int>()))
            {
                Assert.Throws<SchemaRegistryException>(() =>
                {
                    try
                    {
                        producer.ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }).Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            var producerConfig2 = new ProducerConfig { BootstrapServers = bootstrapServers };
            var avroConfig2 = new AvroSerdeProviderConfig { SchemaRegistryUrl = schemaRegistryServers };

            using (var serdeProvider = new AvroSerdeProvider(avroConfig2))
            using (var producer = new Producer<string, int>(producerConfig2, serdeProvider.CreateKeySerializer<string>(), serdeProvider.CreateValueSerializer<int>()))
            {
                producer.ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }).Wait();
            }

            var producerConfig3 = new ProducerConfig { BootstrapServers = bootstrapServers };
            var avroConfig3 = new AvroSerdeProviderConfig { SchemaRegistryUrl = schemaRegistryServers, AvroSerializerAutoRegisterSchemas = false };

            // config with avro.serializer.auto.register.schemas == false should work now.
            using (var serdeProvider = new AvroSerdeProvider(avroConfig3))
            using (var producer = new Producer<string, int>(producerConfig3, serdeProvider.CreateKeySerializer<string>(), serdeProvider.CreateValueSerializer<int>()))
            {
                producer.ProduceAsync(topic, new Message<string, int> { Key = "test", Value = 112 }).Wait();
            }
        }
    }
}
