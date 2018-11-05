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
                var bytesTopic = Guid.NewGuid().ToString();
                var intTopic = Guid.NewGuid().ToString();
                var longTopic = Guid.NewGuid().ToString();
                var boolTopic = Guid.NewGuid().ToString();
                var floatTopic = Guid.NewGuid().ToString();
                var doubleTopic = Guid.NewGuid().ToString();
                var nullTopic = Guid.NewGuid().ToString();

                using (var producer = new AvroProducer(schemaRegistry, producerConfig))
                {
                    producer
                        .ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                
                    producer
                        .ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));

                    producer
                        .ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));

                    producer
                        .ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));

                    producer
                        .ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                
                    producer
                        .ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));

                    producer
                        .ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 }, SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));

                    producer
                        .ProduceAsync(nullTopic, new Message<Null,Null>(), SerdeType.Avro, SerdeType.Avro)
                        .Wait();
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }


                using (var consumer = new AvroConsumer(schemaRegistry, consumerConfig))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.ConsumeAsync<string, string>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result2 = consumer.ConsumeAsync<byte[], byte[]>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal(new byte[] { 1, 4, 11 }, result2.Message.Key);
                    Assert.Equal(new byte[] { }, result2.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result3 = consumer.ConsumeAsync<int, int>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal(42, result3.Message.Key);
                    Assert.Equal(43, result3.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result4 = consumer.ConsumeAsync<long, long>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal(-32, result4.Message.Key);
                    Assert.Equal(-33, result4.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result5 = consumer.ConsumeAsync<bool, bool>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.True(result5.Message.Key);
                    Assert.False(result5.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result6 = consumer.ConsumeAsync<float, float>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal(44.0f, result6.Message.Key);
                    Assert.Equal(45.0f, result6.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result7 = consumer.ConsumeAsync<double, double>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Equal(46.0, result7.Message.Key);
                    Assert.Equal(47.0, result7.Message.Value);

                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(nullTopic, 0, 0) });
                    var result8 = consumer.ConsumeAsync<Null, Null>(SerdeType.Avro, SerdeType.Avro, TimeSpan.FromSeconds(10)).Result;
                    Assert.Null(result8.Key);
                    Assert.Null(result8.Value);
                }
            }
        }
    }
}
