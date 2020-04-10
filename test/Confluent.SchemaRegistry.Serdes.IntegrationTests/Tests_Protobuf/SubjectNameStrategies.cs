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
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that the configuration of the various subject name
        ///     strategies works for Protobuf serializers.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void SubjectNameStrategiesProtobuf(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                using (var producer =
                    new ProducerBuilder<string, UInt32Value>(producerConfig)
                        .SetValueSerializer(new ProtobufSerializer<UInt32Value>(schemaRegistry, new ProtobufSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.TopicRecord }))
                        .Build())
                {
                    var u = new UInt32Value();
                    u.Value = 42;
                    producer.ProduceAsync(topic.Name, new Message<string, UInt32Value> { Key = "test1", Value = u }).Wait();

                    var subjects = schemaRegistry.GetAllSubjectsAsync().Result;
                    Assert.Contains(topic.Name + "-UInt32Value", subjects);
                    Assert.DoesNotContain(topic.Name + "-value", subjects);
                    // May contain the record name subject from a previous test.
                }

                using (var producer =
                    new ProducerBuilder<string, UInt32Value>(producerConfig)
                        .SetValueSerializer(new ProtobufSerializer<UInt32Value>(schemaRegistry, new ProtobufSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Record }))
                        .Build())
                {
                    var u = new UInt32Value();
                    u.Value = 42;
                    producer.ProduceAsync(topic.Name, new Message<string, UInt32Value> { Key = "test1", Value = u }).Wait();

                    var subjects = schemaRegistry.GetAllSubjectsAsync().Result;
                    // Note: If this value is in SR by any means (even if not via this test),
                    // it implies what is being tested here is functional.
                    Assert.Contains("UInt32Value", subjects);
                    Assert.DoesNotContain(topic.Name + "-value", subjects);
                }

                using (var producer =
                    new ProducerBuilder<string, UInt32Value>(producerConfig)
                        .SetValueSerializer(new ProtobufSerializer<UInt32Value>(schemaRegistry))
                        .Build())
                {
                    var u = new UInt32Value();
                    u.Value = 42;
                    producer.ProduceAsync(topic.Name, new Message<string, UInt32Value> { Key = "test1", Value = u }).Wait();

                    var subjects = schemaRegistry.GetAllSubjectsAsync().Result;
                    Assert.Contains(topic.Name + "-value", subjects);
                }
            }
        }
    }
}
