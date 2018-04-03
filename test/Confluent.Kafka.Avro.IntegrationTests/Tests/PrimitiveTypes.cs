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
                producer.ProduceAsync(stringTopic, new Message<string, string> { Key = "hello", Value = "world" });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var bytesTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<byte[], byte[]>(producerConfig, new AvroSerializer<byte[]>(), new AvroSerializer<byte[]>()))
            {
                producer.ProduceAsync(bytesTopic, new Message<byte[], byte[]> { Key = new byte[] { 1, 4, 11 }, Value = new byte[] {} });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var intTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<int, int>(producerConfig, new AvroSerializer<int>(), new AvroSerializer<int>()))
            {
                producer.ProduceAsync(intTopic, new Message<int, int> { Key = 42, Value = 43 });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var longTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<long, long>(producerConfig, new AvroSerializer<long>(), new AvroSerializer<long>()))
            {
                producer.ProduceAsync(longTopic, new Message<long, long> { Key = -32, Value = -33 });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var boolTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<bool, bool>(producerConfig, new AvroSerializer<bool>(), new AvroSerializer<bool>()))
            {
                producer.ProduceAsync(boolTopic, new Message<bool, bool> { Key = true, Value = false });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var floatTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<float, float>(producerConfig, new AvroSerializer<float>(), new AvroSerializer<float>()))
            {
                producer.ProduceAsync(floatTopic, new Message<float, float> { Key = 44.0f, Value = 45.0f });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var doubleTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<double, double>(producerConfig, new AvroSerializer<double>(), new AvroSerializer<double>()))
            {
                producer.ProduceAsync(doubleTopic, new Message<double, double> { Key = 46.0, Value = 47.0 });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var nullTopic = Guid.NewGuid().ToString();
            using (var producer = new Producer<Null,Null>(producerConfig, new AvroSerializer<Null>(), new AvroSerializer<Null>()))
            {
                producer.ProduceAsync(nullTopic, new Message<Null,Null>());
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new Consumer<string, string>(consumerConfig, new AvroDeserializer<string>(), new AvroDeserializer<string>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(stringTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<string, string> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal("hello", record.Message.Key);
                Assert.Equal("world", record.Message.Value);
            }

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new AvroDeserializer<byte[]>(), new AvroDeserializer<byte[]>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(bytesTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<byte[], byte[]> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal(new byte[] { 1, 4, 11 }, record.Message.Key);
                Assert.Equal(new byte[] { }, record.Message.Value);
            }

            using (var consumer = new Consumer<int, int>(consumerConfig, new AvroDeserializer<int>(), new AvroDeserializer<int>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(intTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<int, int> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal(42, record.Message.Key);
                Assert.Equal(43, record.Message.Value);
            }

            using (var consumer = new Consumer<long, long>(consumerConfig, new AvroDeserializer<long>(), new AvroDeserializer<long>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(longTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<long, long> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal(-32, record.Message.Key);
                Assert.Equal(-33, record.Message.Value);
            }

            using (var consumer = new Consumer<bool, bool>(consumerConfig, new AvroDeserializer<bool>(), new AvroDeserializer<bool>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(boolTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<bool, bool> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.True(record.Message.Key);
                Assert.False(record.Message.Value);
            }

            using (var consumer = new Consumer<float, float>(consumerConfig, new AvroDeserializer<float>(), new AvroDeserializer<float>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(floatTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<float, float> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal(44.0f, record.Message.Key);
                Assert.Equal(45.0f, record.Message.Value);
            }

            using (var consumer = new Consumer<double, double>(consumerConfig, new AvroDeserializer<double>(), new AvroDeserializer<double>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(doubleTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<double, double> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Equal(46.0, record.Message.Key);
                Assert.Equal(47.0, record.Message.Value);
            }

            using (var consumer = new Consumer<Null, Null>(consumerConfig, new AvroDeserializer<Null>(), new AvroDeserializer<Null>()))
            {
                consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(nullTopic, 0, 0) });
                consumer.Consume(out ConsumerRecord<Null, Null> record, TimeSpan.FromSeconds(10));
                Assert.Equal(ErrorCode.NoError, record.Error.Code);
                Assert.Null(record.Key);
                Assert.Null(record.Value);
            }
        }
    }
}
