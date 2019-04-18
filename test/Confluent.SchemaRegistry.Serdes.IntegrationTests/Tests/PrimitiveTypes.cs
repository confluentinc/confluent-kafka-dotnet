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
using System.Threading;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test AvroSerializer and AvroDeserializer work with all supported
        ///     primitive types.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void PrimitiveTypes(string bootstrapServers, string schemaRegistryServers)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryServers };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = Guid.NewGuid().ToString(),
                    SessionTimeoutMs = 6000,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                var stringTopic = Guid.NewGuid().ToString();
                var bytesTopic = Guid.NewGuid().ToString();
                var intTopic = Guid.NewGuid().ToString();
                var longTopic = Guid.NewGuid().ToString();
                var boolTopic = Guid.NewGuid().ToString();
                var floatTopic = Guid.NewGuid().ToString();
                var doubleTopic = Guid.NewGuid().ToString();
                var nullTopic = Guid.NewGuid().ToString();

                using (var producer =
                    new ProducerBuilder<string, string>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<string>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<string>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<byte[], byte[]>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<byte[]>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<byte[]>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<int, int>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<int>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<int>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<long, long>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<long>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<long>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<bool, bool>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<bool>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<bool>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<float, float>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<float>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<float>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<double, double>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<double>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<double>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer =
                    new ProducerBuilder<Null, Null>(producerConfig)
                        .SetKeySerializer(new AsyncAvroSerializer<Null>(schemaRegistry))
                        .SetValueSerializer(new AsyncAvroSerializer<Null>(schemaRegistry))
                        .Build())
                {
                    producer
                        .ProduceAsync(nullTopic, new Message<Null,Null>())
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }


                using (var consumer =
                    new ConsumerBuilder<string, string>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<byte[]>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<byte[]>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 1, 4, 11 }, result2.Message.Key);
                    Assert.Equal(new byte[] { }, result2.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<int, int>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<int>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<int>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, result3.Message.Key);
                    Assert.Equal(43, result3.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<long, long>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<long>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<long>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(-32, result4.Message.Key);
                    Assert.Equal(-33, result4.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<bool, bool>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<bool>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<bool>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result5 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.True(result5.Message.Key);
                    Assert.False(result5.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<float, float>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<float>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<float>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result6 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0f, result6.Message.Key);
                    Assert.Equal(45.0f, result6.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<double, double>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<double>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<double>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result7 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(46.0, result7.Message.Key);
                    Assert.Equal(47.0, result7.Message.Value);
                }

                using (var consumer =
                    new ConsumerBuilder<Null, Null>(consumerConfig)
                        .SetKeyDeserializer(new AsyncAvroDeserializer<Null>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AsyncAvroDeserializer<Null>(schemaRegistry).AsSyncOverAsync())
                        .Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(nullTopic, 0, 0) });
                    var result8 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Null(result8.Key);
                    Assert.Null(result8.Value);
                }
            }
        }
    }
}
