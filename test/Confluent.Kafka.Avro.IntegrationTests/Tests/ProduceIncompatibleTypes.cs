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
        ///     Test that producing messages with a key or value with incompatible schema
        ///     throws a SchemaRegistryException.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceIncompatibleTypes(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid().ToString() },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" },
                { "schema.registry.url", schemaRegistryServers }
            };

            var topic = Guid.NewGuid().ToString();
            using (var producer = new Producer<string, string>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<string>()))
            {
                producer.ProduceAsync(topic, new Message<string, string> { Key = "hello", Value = "world" });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var producer = new Producer<int, string>(producerConfig, new AvroSerializer<int>(), new AvroSerializer<string>()))
            {
                Assert.Throws<SchemaRegistryException>(() =>
                {
                    try
                    {
                        producer.ProduceAsync(topic, new Message<int, string> { Key = 42, Value = "world" }).Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

            using (var producer = new Producer<string, int>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<int>()))
            {
                Assert.Throws<SchemaRegistryException>(() =>
                {
                    try
                    {
                        producer.ProduceAsync(topic, new Message<string, int> { Key = "world", Value = 42 }).Wait();
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
