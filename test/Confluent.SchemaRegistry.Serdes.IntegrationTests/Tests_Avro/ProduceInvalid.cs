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
        ///     Test producing non-record type using TopicRecord strategy fails (value)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceInvalid_NotRecord1(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            // 1. TopicRecord naming strategy only works with avro record types (value)
            var schemaRegistryConfig1 = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };
            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig1))
            using (var producer =
                new ProducerBuilder<string, string>(producerConfig)
                    // not a record type.
                    .SetValueSerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Exception caught = null;
                try
                {
                    producer.ProduceAsync(topic, new Message<string, string> { Key = "hello", Value = "world" }).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    caught = e;
                }

                Assert.NotNull(caught);
                Assert.IsType<ProduceException<string, string>>(caught);
                Assert.Equal(ErrorCode.Local_ValueSerialization, ((ProduceException<string, string>)caught).Error.Code);
            }
        }

        /// <summary>
        ///     Test producing non-record type using Record strategy fails (value)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceInvalid_NotRecord2(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            // 2. Record naming strategy only works with avro record types (value)
            var schemaRegistryConfig2 = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };
            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Record
            };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig2))
            using (var producer =
                new ProducerBuilder<string, string>(producerConfig)
                    // not a record type.
                    .SetValueSerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Exception caught = null;
                try
                {
                    producer.ProduceAsync(topic, new Message<string, string> { Key = "hello", Value = "world" }).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    caught = e;
                }

                Assert.NotNull(caught);
                Assert.IsType<ProduceException<string, string>>(caught);
                Assert.Equal(ErrorCode.Local_ValueSerialization, ((ProduceException<string, string>)caught).Error.Code);
            }
        }

        /// <summary>
        ///     Test producing non-record type using TopicRecord strategy fails (key)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceInvalid_NotRecord3(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            // 3. TopicRecord naming strategy only works with avro record types (key)
            var schemaRegistryConfig3 = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };
            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig3))
            using (var producer =
                new ProducerBuilder<string, string>(producerConfig)
                    // not a record type.
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Exception caught = null;
                try
                {
                    producer.ProduceAsync(topic, new Message<string, string> { Key = "hello", Value = "world" }).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    caught = e;
                }

                Assert.NotNull(caught);
                Assert.IsType<ProduceException<string, string>>(caught);
                Assert.Equal(ErrorCode.Local_KeySerialization, ((ProduceException<string, string>)caught).Error.Code);
            }
        }

        /// <summary>
        ///     Test producing non-record type using Record strategy fails (key)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceInvalid_NotRecord4(string bootstrapServers, string schemaRegistryServers)
        {
            string topic = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            // 2. Record naming strategy only works with avro record types (key)
            var schemaRegistryConfig4 = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };
            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Record
            };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig4))
            using (var producer =
                new ProducerBuilder<string, string>(producerConfig)
                    // not a record type.
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Exception caught = null;
                try
                {
                    producer.ProduceAsync(topic, new Message<string, string> { Key = "hello", Value = "world" }).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    caught = e;
                }

                Assert.NotNull(caught);
                Assert.IsType<ProduceException<string, string>>(caught);
                Assert.Equal(ErrorCode.Local_KeySerialization, ((ProduceException<string, string>)caught).Error.Code);
            }
        }
    }
}
