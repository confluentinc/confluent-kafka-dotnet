// Copyright 2020 Confluent Inc.
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

using Xunit;
using System;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test of producing/consuming using the protobuf serdes and
        ///     schema that references another schema.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceConsumeExternalRefProtobuf(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, WithExternalReference>(producerConfig)
                    .SetValueSerializer(new ProtobufSerializer<WithExternalReference>(schemaRegistry))
                    .Build())
            {
                var u = new WithExternalReference();
                u.Value1 = new RefByAnother { Value = 111 };
                u.Value2 = 123;
                producer.ProduceAsync(topic.Name, new Message<string, WithExternalReference> { Key = "test1", Value = u }).Wait();

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString(),
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                // Test the protobuf deserializer can read this message
                using (var consumer =
                    new ConsumerBuilder<string, WithExternalReference>(consumerConfig)
                        .SetValueDeserializer(new ProtobufDeserializer<WithExternalReference>().AsSyncOverAsync())
                        .Build())
                {
                    consumer.Subscribe(topic.Name);
                    var cr = consumer.Consume();
                    Assert.Equal(u.Value2, cr.Message.Value.Value2);
                    Assert.Equal(u.Value1.Value, cr.Message.Value.Value1.Value);
                }

                // Check the pre-data bytes are as expected.
                using (var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build())
                {
                    consumer.Subscribe(topic.Name);
                    var cr = consumer.Consume();
                    // magic byte + schema id + expected array index length + at least one data byte.
                    Assert.True(cr.Message.Value.Length >= 1 + 4 + 1 + 1);
                    // magic byte
                    Assert.Equal(0, cr.Message.Value[0]);
                    // array index (special value as an optimization)
                    Assert.Equal(0, cr.Message.Value[5]);
                }

                // Check the referenced schema is in schema registry.
                var subjects = schemaRegistry.GetAllSubjectsAsync().Result;
                Assert.Contains("RefByAnother.proto", subjects);
            }
        }
    }
}
