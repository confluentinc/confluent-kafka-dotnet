﻿// Copyright 2018 Confluent Inc.
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
using Confluent.Kafka.AvroSerdes;
using Confluent.SchemaRegistry;
using Xunit;


namespace Confluent.Kafka.Avro.IntegrationTests
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
                    AutoOffsetReset = AutoOffsetResetType.Earliest
                };

                var stringTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<string>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            stringTopic, new Message<string, string> { Key = "hello", Value = "world" })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var bytesTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<byte[]>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var intTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<int>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            bytesTopic, new Message<int, int> { Key = 42, Value = 43 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var longTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<long>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            bytesTopic, new Message<long, long> { Key = -32, Value = -33 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var boolTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<bool>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            boolTopic, new Message<bool, bool> { Key = true, Value = false })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var floatTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<float>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var doubleTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<double>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var nullTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer(producerConfig))
                {
                    var serializer = new AvroSerializer<Null>(schemaRegistry);
                    producer
                        .ProduceAsync(
                            serializer, serializer,
                            nullTopic, new Message<Null,Null>())
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }


                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.Consume<string, string>(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result = consumer.Consume<byte[], byte[]>(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 1, 4, 11 }, result.Message.Key);
                    Assert.Equal(new byte[] { }, result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result = consumer.Consume<int, int>(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, result.Message.Key);
                    Assert.Equal(43, result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result = consumer.Consume<int, int>(TimeSpan.FromSeconds(10));
                    Assert.Equal(-32, result.Message.Key);
                    Assert.Equal(-33, result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result = consumer.Consume<bool, bool>(TimeSpan.FromSeconds(10));
                    Assert.True(result.Message.Key);
                    Assert.False(result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result = consumer.Consume<float, float>(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0f, result.Message.Key);
                    Assert.Equal(45.0f, result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result = consumer.Consume<double, double>(TimeSpan.FromSeconds(10));
                    Assert.Equal(46.0, result.Message.Key);
                    Assert.Equal(47.0, result.Message.Value);
                }

                using (var consumer = new Consumer(consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(nullTopic, 0, 0) });
                    var result = consumer.Consume<Null, Null>(TimeSpan.FromSeconds(10));
                    Assert.Null(result.Key);
                    Assert.Null(result.Value);
                }
            }
        }
    }
}
