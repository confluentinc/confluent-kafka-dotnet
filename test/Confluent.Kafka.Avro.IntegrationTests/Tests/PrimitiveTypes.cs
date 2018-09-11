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
                using (var producer = new Producer<string, string>(producerConfig, serdeProvider.CreateKeySerializer<string>(), serdeProvider.CreateValueSerializer<string>()))
                {
                    producer.ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var bytesTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<byte[], byte[]>(producerConfig, serdeProvider.CreateKeySerializer<byte[]>(), serdeProvider.CreateValueSerializer<byte[]>()))
                {
                    producer.ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var intTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<int, int>(producerConfig, serdeProvider.CreateKeySerializer<int>(), serdeProvider.CreateValueSerializer<int>()))
                {
                    producer.ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var longTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<long, long>(producerConfig, serdeProvider.CreateKeySerializer<long>(), serdeProvider.CreateValueSerializer<long>()))
                {
                    producer.ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var boolTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<bool, bool>(producerConfig, serdeProvider.CreateKeySerializer<bool>(), serdeProvider.CreateValueSerializer<bool>()))
                {
                    producer.ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var floatTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<float, float>(producerConfig, serdeProvider.CreateKeySerializer<float>(), serdeProvider.CreateValueSerializer<float>()))
                {
                    producer.ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var doubleTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<double, double>(producerConfig, serdeProvider.CreateKeySerializer<double>(), serdeProvider.CreateValueSerializer<double>()))
                {
                    producer.ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                var nullTopic = Guid.NewGuid().ToString();
                using (var producer = new Producer<Null,Null>(producerConfig, serdeProvider.CreateKeySerializer<Null>(), serdeProvider.CreateValueSerializer<Null>()))
                {
                    producer.ProduceAsync(nullTopic, new Message<Null,Null>());
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                }

                using (var consumer = new Consumer<string, string>(consumerConfig, serdeProvider.CreateDeserializer<string>(), serdeProvider.CreateDeserializer<string>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", result.Message.Key);
                    Assert.Equal("world", result.Message.Value);
                }

                using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, serdeProvider.CreateDeserializer<byte[]>(), serdeProvider.CreateDeserializer<byte[]>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 1, 4, 11 }, result.Message.Key);
                    Assert.Equal(new byte[] { }, result.Message.Value);
                }

                using (var consumer = new Consumer<int, int>(consumerConfig, serdeProvider.CreateDeserializer<int>(), serdeProvider.CreateDeserializer<int>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, result.Message.Key);
                    Assert.Equal(43, result.Message.Value);
                }

                using (var consumer = new Consumer<long, long>(consumerConfig, serdeProvider.CreateDeserializer<long>(), serdeProvider.CreateDeserializer<long>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(-32, result.Message.Key);
                    Assert.Equal(-33, result.Message.Value);
                }

                using (var consumer = new Consumer<bool, bool>(consumerConfig, serdeProvider.CreateDeserializer<bool>(), serdeProvider.CreateDeserializer<bool>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.True(result.Message.Key);
                    Assert.False(result.Message.Value);
                }

                using (var consumer = new Consumer<float, float>(consumerConfig, serdeProvider.CreateDeserializer<float>(), serdeProvider.CreateDeserializer<float>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0f, result.Message.Key);
                    Assert.Equal(45.0f, result.Message.Value);
                }

                using (var consumer = new Consumer<double, double>(consumerConfig, serdeProvider.CreateDeserializer<double>(), serdeProvider.CreateDeserializer<double>()))
                {
                    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                    var result = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(46.0, result.Message.Key);
                    Assert.Equal(47.0, result.Message.Value);
                }

                using (var consumer = new Consumer<Null, Null>(consumerConfig, serdeProvider.CreateDeserializer<Null>(), serdeProvider.CreateDeserializer<Null>()))
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
