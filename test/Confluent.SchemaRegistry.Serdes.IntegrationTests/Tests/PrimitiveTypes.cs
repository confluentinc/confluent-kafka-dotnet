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
                    AutoOffsetReset = AutoOffsetResetType.Earliest
                };

                var stringTopic = Guid.NewGuid().ToString();
                var bytesTopic = Guid.NewGuid().ToString();
                var intTopic = Guid.NewGuid().ToString();
                var longTopic = Guid.NewGuid().ToString();
                var boolTopic = Guid.NewGuid().ToString();
                var floatTopic = Guid.NewGuid().ToString();
                var doubleTopic = Guid.NewGuid().ToString();
                var nullTopic = Guid.NewGuid().ToString();

                using (var producer = new Producer<string, string>(
                    producerConfig, new AvroSerializer<string>(schemaRegistry), new AvroSerializer<string>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<byte[], byte[]>(
                    producerConfig, new AvroSerializer<byte[]>(schemaRegistry), new AvroSerializer<byte[]>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<int, int>(
                    producerConfig, new AvroSerializer<int>(schemaRegistry), new AvroSerializer<int>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<long, long>(
                    producerConfig, new AvroSerializer<long>(schemaRegistry), new AvroSerializer<long>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<bool, bool>(
                    producerConfig, new AvroSerializer<bool>(schemaRegistry), new AvroSerializer<bool>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<float, float>(
                    producerConfig, new AvroSerializer<float>(schemaRegistry), new AvroSerializer<float>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<double, double>(
                    producerConfig, new AvroSerializer<double>(schemaRegistry), new AvroSerializer<double>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 })
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var producer = new Producer<Null, Null>(
                    producerConfig, new AvroSerializer<Null>(schemaRegistry), new AvroSerializer<Null>(schemaRegistry)))
                {
                    producer
                        .ProduceAsync(nullTopic, new Message<Null,Null>())
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }


                using (var consumer = new Consumer<string, string>(
                    consumerConfig, new AvroDeserializer<string>(schemaRegistry), new AvroDeserializer<string>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);
                }

                using (var consumer = new Consumer<byte[], byte[]>(
                    consumerConfig, new AvroDeserializer<byte[]>(schemaRegistry), new AvroDeserializer<byte[]>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 1, 4, 11 }, result2.Message.Key);
                    Assert.Equal(new byte[] { }, result2.Message.Value);
                }

                using (var consumer = new Consumer<int, int>(
                    consumerConfig, new AvroDeserializer<int>(schemaRegistry), new AvroDeserializer<int>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, result3.Message.Key);
                    Assert.Equal(43, result3.Message.Value);
                }

                using (var consumer = new Consumer<long, long>(
                    consumerConfig, new AvroDeserializer<long>(schemaRegistry), new AvroDeserializer<long>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(-32, result4.Message.Key);
                    Assert.Equal(-33, result4.Message.Value);
                }

                using (var consumer = new Consumer<bool, bool>(
                    consumerConfig, new AvroDeserializer<bool>(schemaRegistry), new AvroDeserializer<bool>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result5 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.True(result5.Message.Key);
                    Assert.False(result5.Message.Value);
                }

                using (var consumer = new Consumer<float, float>(
                    consumerConfig, new AvroDeserializer<float>(schemaRegistry), new AvroDeserializer<float>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result6 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0f, result6.Message.Key);
                    Assert.Equal(45.0f, result6.Message.Value);
                }

                using (var consumer = new Consumer<double, double>(
                    consumerConfig, new AvroDeserializer<double>(schemaRegistry), new AvroDeserializer<double>(schemaRegistry)))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result7 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(46.0, result7.Message.Key);
                    Assert.Equal(47.0, result7.Message.Value);
                }

                using (var consumer = new Consumer<Null, Null>(
                    consumerConfig, new AvroDeserializer<Null>(schemaRegistry), new AvroDeserializer<Null>(schemaRegistry)))
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
