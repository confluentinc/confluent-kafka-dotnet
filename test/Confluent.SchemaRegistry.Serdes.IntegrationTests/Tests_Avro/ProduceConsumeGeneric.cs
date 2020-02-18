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
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.SyncOverAsync;
using Avro;
using Avro.Generic;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        private static void ProduceConsumeGeneric(string bootstrapServers, string schemaRegistryServers, SubjectNameStrategy nameStrategy)
        {
            var rs = (RecordSchema)RecordSchema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""ProduceConsumeUser2"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers,
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = nameStrategy
            };

            var topic = Guid.NewGuid().ToString();

            DeliveryResult<GenericRecord, Null> dr;
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var p =
                new ProducerBuilder<GenericRecord, Null>(config)
                    // Test KeySubjectNameStrategy here,
                    // and ValueSubjectNameStrategy in ProduceConsume.
                    .SetKeySerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(Serializers.Null)
                    .Build())
            {
                var record = new GenericRecord(rs);
                record.Add("name", "my name 2");
                record.Add("favorite_number", 44);
                record.Add("favorite_color", null);
                dr = p.ProduceAsync(topic, new Message<GenericRecord, Null> { Key = record }).Result;
            }

            // produce a specific record (to later consume back as a generic record).
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var p =
                new ProducerBuilder<ProduceConsumeUser2, Null>(config)
                    .SetKeySerializer(new AvroSerializer<ProduceConsumeUser2>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(Serializers.Null)
                    .Build())
            {
                var user = new ProduceConsumeUser2
                {
                    name = "my name 3",
                    favorite_number = 47,
                    favorite_color = "orange"
                };
                
                p.ProduceAsync(topic, new Message<ProduceConsumeUser2, Null> { Key = user }).Wait();
            }

            Assert.Null(dr.Message.Value);
            Assert.NotNull(dr.Message.Key);
            dr.Message.Key.TryGetValue("name", out object name);
            dr.Message.Key.TryGetValue("favorite_number", out object number);
            dr.Message.Key.TryGetValue("favorite_color", out object color);

            Assert.IsType<string>(name);
            Assert.IsType<int>(number);

            Assert.Equal("my name 2", name);
            Assert.Equal(44, number);
            Assert.Null(color);

            var cconfig = new ConsumerConfig { GroupId = Guid.NewGuid().ToString(), BootstrapServers = bootstrapServers };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<GenericRecord, Null>(cconfig)
                    .SetKeyDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(Deserializers.Null)
                    .Build())
            {
                // consume generic record produced as a generic record.
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                var record = consumer.Consume(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
                record.Message.Key.TryGetValue("name", out object msgName);
                record.Message.Key.TryGetValue("favorite_number", out object msgNumber);
                record.Message.Key.TryGetValue("favorite_color", out object msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);

                Assert.Equal("my name 2", msgName);
                Assert.Equal(44, msgNumber);
                Assert.Null(msgColor);

                // consume generic record produced as a specific record.
                record = consumer.Consume(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
                record.Message.Key.TryGetValue("name", out msgName);
                record.Message.Key.TryGetValue("favorite_number", out msgNumber);
                record.Message.Key.TryGetValue("favorite_color", out msgColor);

                Assert.IsType<string>(msgName);
                Assert.IsType<int>(msgNumber);
                Assert.IsType<string>(msgColor);

                Assert.Equal("my name 3", msgName);
                Assert.Equal(47, msgNumber);
                Assert.Equal("orange", msgColor);
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<ProduceConsumeUser2, Null>(cconfig)
                    .SetKeyDeserializer(new AvroDeserializer<ProduceConsumeUser2>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(Deserializers.Null)
                    .Build())
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, dr.Offset) });
                var record = consumer.Consume(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
                Assert.Equal("my name 2", record.Message.Key.name);
                Assert.Equal(44, record.Message.Key.favorite_number);
                Assert.Null(record.Message.Key.favorite_color);
            }

            // Check that what's in schema registry is what's expected.
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                var subjects = schemaRegistry.GetAllSubjectsAsync().Result;

                if (nameStrategy == SubjectNameStrategy.TopicRecord)
                {
                    Assert.Single(subjects.Where(s => s.Contains(topic)));
                    Assert.Single(subjects.Where(s => s == $"{topic}-{((Avro.RecordSchema)ProduceConsumeUser2._SCHEMA).Fullname}"));
                }

                if (nameStrategy == SubjectNameStrategy.Topic)
                {
                    Assert.Single(subjects.Where(s => s.Contains(topic)));
                    Assert.Single(subjects.Where(s => s == $"{topic}-key"));
                }

                if (nameStrategy == SubjectNameStrategy.Record)
                {
                    Assert.Single(subjects.Where(s => s.Contains(topic))); // the string key.
                    Assert.Single(subjects.Where(s => s == $"{((Avro.RecordSchema)ProduceConsumeUser2._SCHEMA).Fullname}"));
                }
            }
        }
        
        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic name strategy).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeGeneric_Topic(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeGeneric(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Topic);
        }

        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (topic record name strategy).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeGeneric_TopicRecord(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeGeneric(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.TopicRecord);
        }

        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer (record name strategy).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceConsumeGeneric_Record(string bootstrapServers, string schemaRegistryServers)
        {
            ProduceConsumeGeneric(bootstrapServers, schemaRegistryServers, SubjectNameStrategy.Topic);
        }
    }
}
