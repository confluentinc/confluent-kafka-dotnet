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
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid().ToString() },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" },
                { "schema.registry.url", schemaRegistryServers }
            };

            var stringTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<string, string>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<string>()))
            {
                producer.ProduceAsync(stringTopic, "hello", "world");
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var bytesTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<byte[], byte[]>(producerConfig, new AvroSerializer<byte[]>(), new AvroSerializer<byte[]>()))
            {
                producer.ProduceAsync(bytesTopic, new byte[] { 1, 4, 11 }, new byte[] { });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var intTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<int, int>(producerConfig, new AvroSerializer<int>(), new AvroSerializer<int>()))
            {
                producer.ProduceAsync(intTopic, 42, 43);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var longTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<long, long>(producerConfig, new AvroSerializer<long>(), new AvroSerializer<long>()))
            {
                producer.ProduceAsync(longTopic, -32, -33);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var boolTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<bool, bool>(producerConfig, new AvroSerializer<bool>(), new AvroSerializer<bool>()))
            {
                producer.ProduceAsync(boolTopic, true, false);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var floatTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<float, float>(producerConfig, new AvroSerializer<float>(), new AvroSerializer<float>()))
            {
                producer.ProduceAsync(floatTopic, 44.0f, 45.0f);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var doubleTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<double, double>(producerConfig, new AvroSerializer<double>(), new AvroSerializer<double>()))
            {
                producer.ProduceAsync(doubleTopic, 46.0, 47.0);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new Consumer<string, string>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<string>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                consumer.Consume(out Message<string, string> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal("hello", message.Key);
                Assert.Equal("world", message.Value);
            }

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new AvroDeserializer<byte[]>(), new AvroDeserializer<byte[]>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                consumer.Consume(out Message<byte[], byte[]> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(new byte[] { 1, 4, 11 }, message.Key);
                Assert.Equal(new byte[] { }, message.Value);
            }

            using (var consumer = new Consumer<int, int>(consumerConfig, new AvroDeserializer<int>(), new AvroDeserializer<int>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                consumer.Consume(out Message<int, int> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(42, message.Key);
                Assert.Equal(43, message.Value);
            }

            using (var consumer = new Consumer<long, long>(consumerConfig, new AvroDeserializer<long>(), new AvroDeserializer<long>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                consumer.Consume(out Message<long, long> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(-32, message.Key);
                Assert.Equal(-33, message.Value);
            }

            using (var consumer = new Consumer<bool, bool>(consumerConfig, new AvroDeserializer<bool>(), new AvroDeserializer<bool>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                consumer.Consume(out Message<bool, bool> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(true, message.Key);
                Assert.Equal(false, message.Value);
            }

            using (var consumer = new Consumer<float, float>(consumerConfig, new AvroDeserializer<float>(), new AvroDeserializer<float>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                consumer.Consume(out Message<float, float> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(44.0f, message.Key);
                Assert.Equal(45.0f, message.Value);
            }

            using (var consumer = new Consumer<double, double>(consumerConfig, new AvroDeserializer<double>(), new AvroDeserializer<double>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                consumer.Consume(out Message<double, double> message, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, message.Error.Code);
                Assert.Equal(46.0, message.Key);
                Assert.Equal(47.0, message.Value);
            }
        }
    }
}
