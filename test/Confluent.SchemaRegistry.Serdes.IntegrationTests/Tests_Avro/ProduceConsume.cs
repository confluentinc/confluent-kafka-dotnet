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
        private static void ProduceConsume(string bootstrapServers, string schemaRegistryServers, SubjectNameStrategy nameStrategy)
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
                    var user = new ProduceConsumeUser
                    {
                        name = i.ToString(),
                        favorite_number = i,
                        favorite_color = "blue"
                    };
                    
                    producer
                        .ProduceAsync(topic, new Message<string, ProduceConsumeUser> { Key = user.name, Value = user })
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

                    Assert.Equal(i.ToString(), record.Message.Key);
                    Assert.Equal(i.ToString(), record.Message.Value.name);
                    Assert.Equal(i, record.Message.Value.favorite_number);
                    Assert.Equal("blue", record.Message.Value.favorite_color);
                    i += 1;
                }

                Assert.Equal(100, i);

                consumer.Close();
            }

            // Check that what's in schema registry is what's expected.
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                var subjects = schemaRegistry.GetAllSubjectsAsync().Result;

                if (nameStrategy == SubjectNameStrategy.TopicRecord)
                {
                    Assert.Equal(2, (int)subjects.Where(s => s.Contains(topic)).Count());
                    Assert.Single(subjects.Where(s => s == $"{topic}-key"));
                    Assert.Single(subjects.Where(s => s == $"{topic}-{((Avro.RecordSchema)ProduceConsumeUser._SCHEMA).Fullname}"));
                }

                if (nameStrategy == SubjectNameStrategy.Topic)
                {
                    Assert.Equal(2, (int)subjects.Where(s => s.Contains(topic)).Count());
                    Assert.Single(subjects.Where(s => s == $"{topic}-key"));
                    Assert.Single(subjects.Where(s => s == $"{topic}-value"));
                }

                if (nameStrategy == SubjectNameStrategy.Record)
                {
                    Assert.Single(subjects.Where(s => s.Contains(topic))); // the string key.
                    Assert.Single(subjects.Where(s => s == $"{topic}-key"));
                    Assert.Single(subjects.Where(s => s == $"{((Avro.RecordSchema)ProduceConsumeUser._SCHEMA).Fullname}"));
                }
            }
        }

        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic name strategy)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsume_Topic(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsume(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Topic);
        }

        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic record name strategy)
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsume_TopicRecord(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsume(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.TopicRecord);
        }

        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (record name strategy).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsume_Record(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsume(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Record);
        }
    }
}
