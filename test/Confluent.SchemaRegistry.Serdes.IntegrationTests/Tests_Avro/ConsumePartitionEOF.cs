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
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.Kafka.Examples.AvroSpecific;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test PartitionEOF functionality in the case of IAsycDeserializer
        ///     (which has different code path within Consumer).
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ConsumePartitionEOF(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<Null, User>(producerConfig)
                    .SetKeySerializer(Serializers.Null)
                    .SetValueSerializer(new AvroSerializer<User>(schemaRegistry))
                    .Build())
            {
                producer.ProduceAsync(topic.Name, new Message<Null, User> { Value = new User { name = "test" } }).Wait();

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString(),
                    SessionTimeoutMs = 6000,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnablePartitionEof = true
                };

                using (var consumer =
                    new ConsumerBuilder<Null, User>(consumerConfig)
                        .SetKeyDeserializer(Deserializers.Null)
                        .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
                        .SetPartitionsAssignedHandler((c, partitions)
                            => partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)))
                        .Build())
                {
                    consumer.Subscribe(topic.Name);

                    var cr1 = consumer.Consume();
                    Assert.NotNull(cr1);
                    Assert.NotNull(cr1.Message);
                    Assert.False(cr1.IsPartitionEOF);
                    var cr2 = consumer.Consume();
                    Assert.NotNull(cr2);
                    Assert.Null(cr2.Message);
                    Assert.True(cr2.IsPartitionEOF);
                }

                consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString(),
                    SessionTimeoutMs = 6000,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnablePartitionEof = false
                };

                using (var consumer =
                    new ConsumerBuilder<Null, User>(consumerConfig)
                        .SetKeyDeserializer(Deserializers.Null)
                        .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync())
                        .SetPartitionsAssignedHandler((c, partitions)
                            => partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)))
                        .Build())
                {
                    consumer.Subscribe(topic.Name);

                    var cr1 = consumer.Consume();
                    Assert.NotNull(cr1);
                    Assert.NotNull(cr1.Message);
                    Assert.False(cr1.IsPartitionEOF);
                    var cr2 = consumer.Consume(TimeSpan.FromSeconds(2));
                    Assert.Null(cr2);
                }
            }
        }
    }
}
