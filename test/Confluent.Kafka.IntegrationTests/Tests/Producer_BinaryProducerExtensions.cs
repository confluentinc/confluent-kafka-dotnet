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

#pragma warning disable xUnit1026

using System;
using System.Buffers.Binary;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test a variety of cases where a producer is constructed
    ///     using the handle from another producer.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_ShortKey(string bootstrapServers)
        {
            LogToFile("start Producer_Binary_ShortKey");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const short key1 = 25678;
            const short key2 = -25678;
            const short key3 = short.MaxValue;
            const short key4 = short.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(new TopicPartition(topic.Name, 0), key2, value);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(new TopicPartition(topic.Name, 0), key4, value);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<short, byte[]>(consumerConfig)
                           .SetKeyDeserializer(new ShortDeserializer())
                           .Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key1, r1.Message.Key);

                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key2, r2.Message.Key);

                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key3, r3.Message.Key);

                    var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key4, r4.Message.Key);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Binary_ShortKey");
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_IntKey(string bootstrapServers)
        {
            LogToFile("start Producer_Binary_IntKey");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const int key1 = 256780;
            const int key2 = -256780;
            const int key3 = int.MaxValue;
            const int key4 = int.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(new TopicPartition(topic.Name, 0), key2, value);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(new TopicPartition(topic.Name, 0), key4, value);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<int, byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key1, r1.Message.Key);

                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key2, r2.Message.Key);

                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key3, r3.Message.Key);

                    var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key4, r4.Message.Key);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Binary_IntKey");
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_LongKey(string bootstrapServers)
        {
            LogToFile("start Producer_Binary_LongKey");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const long key1 = 25678000000;
            const long key2 = -25678000000;
            const long key3 = long.MaxValue;
            const long key4 = long.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(new TopicPartition(topic.Name, 0), key2, value);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(new TopicPartition(topic.Name, 0), key4, value);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<long, byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key1, r1.Message.Key);

                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key2, r2.Message.Key);

                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key3, r3.Message.Key);

                    var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key4, r4.Message.Key);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Binary_LongKey");
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_StringKey(string bootstrapServers)
        {
            LogToFile("start Producer_Binary_StringKey");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            string key1 = Guid.NewGuid().ToString();
            string key2 = "QWERTYUIO#";
            string key3 = string.Empty;
            string key4 = new string('x', 8000);

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(new TopicPartition(topic.Name, 0), key2, value);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(new TopicPartition(topic.Name, 0), key4, value);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key1, r1.Message.Key);

                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key2, r2.Message.Key);

                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key3, r3.Message.Key);

                    var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key4, r4.Message.Key);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Binary_StringKey");
        }

        private sealed class ShortDeserializer : IDeserializer<short>
        {
            public short Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return BinaryPrimitives.ReadInt16BigEndian(data);
            }
        }
    }
}