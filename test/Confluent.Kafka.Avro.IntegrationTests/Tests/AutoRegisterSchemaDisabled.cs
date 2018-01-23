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

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "avro.serializer.auto.register.schemas", false }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid().ToString() },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            var schemaRegistryConfig = new Dictionary<string, object>
            {
                { "schema.registry.url", schemaRegistryServers }
            };

            using (var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new Producer<string, int>(producerConfig, new AvroSerializer<string>(schemaRegistryClient), new AvroSerializer<int>(schemaRegistryClient)))
            {
                Assert.Throws<SchemaRegistryException>(() =>
                {
                    try
                    {
                        producer.ProduceAsync(topic, "test", 112).Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            var producerConfig2 = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            using (var producer = new Producer<string, int>(producerConfig2, new AvroSerializer<string>(), new AvroSerializer<int>()))
            {
                producer.ProduceAsync(topic, "test", 112).Wait();
            }

            var producerConfig3 = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers },
                { "avro.serializer.auto.register.schemas", false }
            };

            // config with avro.serializer.auto.register.schemas == false should work now.
            using (var producer = new Producer<string, int>(producerConfig3, new AvroSerializer<string>(), new AvroSerializer<int>()))
            {
                producer.ProduceAsync(topic, "test", 112).Wait();
            }
        }
    }
}
