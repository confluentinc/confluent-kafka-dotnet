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
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.SyncOverAsync;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        private static void ProduceConsumeNull(string bootstrapServers, string schemaRegistryServers, SubjectNameStrategy nameStrategy)
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
                Url = schemaRegistryServers,
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = nameStrategy
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
                new ProducerBuilder<string, ProduceConsumeUser>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                    // Test ValueSubjectNameStrategy here,
                    // and KeySubjectNameStrategy in ProduceConsumeGeneric.
                    .SetValueSerializer(new AvroSerializer<ProduceConsumeUser>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                for (int i = 0; i < 100; ++i)
                {
                    producer
                        .ProduceAsync(topic, new Message<string, ProduceConsumeUser> { Key = null, Value = null })
                        .Wait();
                }
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, ProduceConsumeUser>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<ProduceConsumeUser>(schemaRegistry).AsSyncOverAsync())
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

                    Assert.Null(record.Message.Key);
                    Assert.Null(record.Message.Value);
                    i += 1;
                }

                Assert.Equal(100, i);

                consumer.Close();
            }
        }

        /// <summary>
        ///     Test that null messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic name strategy)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeNull_Topic(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeNull(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Topic);
        }

        /// <summary>
        ///     Test that null messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic record name strategy)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeNull_TopicRecord(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeNull(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.TopicRecord);
        }

        /// <summary>
        ///     Test that null messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (record name strategy).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeNull_Record(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeNull(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Record);
        }
    }
}
