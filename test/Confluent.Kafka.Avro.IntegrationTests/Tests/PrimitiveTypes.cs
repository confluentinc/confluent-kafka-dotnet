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
using Confluent.Kafka.Serialization;
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
            var serdeProviderConfig = new AvroSerdeProviderConfig { SchemaRegistryUrl = schemaRegistryServers };

            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
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
                using (var producer = new Producer<string, string>(producerConfig, serdeProvider.GetSerializerGenerator<string>(), serdeProvider.GetSerializerGenerator<string>()))
                {
                    producer.ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var bytesTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<byte[], byte[]>(producerConfig, serdeProvider.GetSerializerGenerator<byte[]>(), serdeProvider.GetSerializerGenerator<byte[]>()))
                {
                    producer.ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var intTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<int, int>(producerConfig, serdeProvider.GetSerializerGenerator<int>(), serdeProvider.GetSerializerGenerator<int>()))
                {
                    producer.ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var longTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<long, long>(producerConfig, serdeProvider.GetSerializerGenerator<long>(), serdeProvider.GetSerializerGenerator<long>()))
                {
                    producer.ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var boolTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<bool, bool>(producerConfig, serdeProvider.GetSerializerGenerator<bool>(), serdeProvider.GetSerializerGenerator<bool>()))
                {
                    producer.ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var floatTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<float, float>(producerConfig, serdeProvider.GetSerializerGenerator<float>(), serdeProvider.GetSerializerGenerator<float>()))
                {
                    producer.ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var doubleTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<double, double>(producerConfig, serdeProvider.GetSerializerGenerator<double>(), serdeProvider.GetSerializerGenerator<double>()))
                {
                    producer.ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var nullTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<Null,Null>(producerConfig, serdeProvider.GetSerializerGenerator<Null>(), serdeProvider.GetSerializerGenerator<Null>()))
                {
                    producer.ProduceAsync(nullTopic, new Message<Null,Null>());
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var consumer = new Consumer<string, string>(consumerConfig, serdeProvider.GetDeserializerGenerator<string>(), serdeProvider.GetDeserializerGenerator<string>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);
                }

                using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, serdeProvider.GetDeserializerGenerator<byte[]>(), serdeProvider.GetDeserializerGenerator<byte[]>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 1, 4, 11 }, result.Message.Key);
                    Assert.Equal(new byte[] { }, result.Message.Value);
                }

                using (var consumer = new Consumer<int, int>(consumerConfig, serdeProvider.GetDeserializerGenerator<int>(), serdeProvider.GetDeserializerGenerator<int>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, result.Message.Key);
                    Assert.Equal(43, result.Message.Value);
                }

                using (var consumer = new Consumer<long, long>(consumerConfig, serdeProvider.GetDeserializerGenerator<long>(), serdeProvider.GetDeserializerGenerator<long>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(-32, result.Message.Key);
                    Assert.Equal(-33, result.Message.Value);
                }

                using (var consumer = new Consumer<bool, bool>(consumerConfig, serdeProvider.GetDeserializerGenerator<bool>(), serdeProvider.GetDeserializerGenerator<bool>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.True(result.Message.Key);
                    Assert.False(result.Message.Value);
                }

                using (var consumer = new Consumer<float, float>(consumerConfig, serdeProvider.GetDeserializerGenerator<float>(), serdeProvider.GetDeserializerGenerator<float>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0f, result.Message.Key);
                    Assert.Equal(45.0f, result.Message.Value);
                }

                using (var consumer = new Consumer<double, double>(consumerConfig, serdeProvider.GetDeserializerGenerator<double>(), serdeProvider.GetDeserializerGenerator<double>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(46.0, result.Message.Key);
                    Assert.Equal(47.0, result.Message.Value);
                }

                using (var consumer = new Consumer<Null, Null>(consumerConfig, serdeProvider.GetDeserializerGenerator<Null>(), serdeProvider.GetDeserializerGenerator<Null>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(nullTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Null(result.Key);
                    Assert.Null(result.Value);
                }
            }
        }
    }
}
