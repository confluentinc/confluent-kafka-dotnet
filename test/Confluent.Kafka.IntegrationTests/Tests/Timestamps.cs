// Copyright 2016-2017 Confluent Inc.
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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Integration tests for Producing / consuming timestamps.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Timestamps(string bootstrapServers)
        {
            LogToFile("start Timestamps");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            var drs_produce = new List<DeliveryReport<Null, string>>();
            var drs_task = new List<DeliveryResult<Null, string>>();
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                // --- ProduceAsync, serializer case.

                drs_task.Add(producer.ProduceAsync(
                    singlePartitionTopic, 
                    new Message<Null, string> { Value = "testvalue" }).Result);
                
                // TimestampType: CreateTime
                drs_task.Add(producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc))
                    }).Result);

                // TimestampType: CreateTime (default)
                drs_task.Add(producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> { Value = "test-value" }).Result);

                // TimestampType: LogAppendTime
                Assert.Throws<AggregateException>(() =>
                    producer.ProduceAsync(
                        new TopicPartition(singlePartitionTopic, 0),
                        new Message<Null, string>
                        {
                            Value = "test-value", 
                            Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) 
                        }).Result);

                // TimestampType: NotAvailable
                Assert.Throws<AggregateException>(() =>
                    producer.ProduceAsync(
                        new TopicPartition(singlePartitionTopic, 0),
                        new Message<Null, string> 
                        { 
                            Value = "test-value",
                            Timestamp = new Timestamp(10, TimestampType.NotAvailable)
                        }).Result);

                Action<DeliveryReport<Null, string>> dh 
                    = (DeliveryReport<Null, string> dr) => drs_produce.Add(dr);


                // --- begin produce, serializer case.

                producer.Produce(
                    singlePartitionTopic,
                    new Message<Null, string> { Value = "testvalue" }, dh);

                // TimestampType: CreateTime
                producer.Produce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc))
                    },
                    dh);

                // TimestampType: CreateTime (default)
                producer.Produce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> { Value = "test-value" },
                    dh);

                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.Produce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime)
                    }, 
                    dh));

                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.Produce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(10, TimestampType.NotAvailable)
                    },
                    dh));

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var drs2_produce = new List<DeliveryReport<byte[], byte[]>>();
            var drs2_task = new List<DeliveryResult<byte[], byte[]>>();
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                // --- ProduceAsync, byte[] case.

                drs2_task.Add(producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = Timestamp.Default }).Result);

                // TimestampType: CreateTime
                drs2_task.Add(producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)) }).Result);

                // TimestampType: CreateTime (default)
                drs2_task.Add(producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = Timestamp.Default }).Result);

                // TimestampType: LogAppendTime
                Assert.Throws<AggregateException>(() =>
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<byte[], byte[]> { Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) }).Result);

                // TimestampType: NotAvailable
                Assert.Throws<AggregateException>(() =>
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<byte[], byte[]> { Timestamp = new Timestamp(10, TimestampType.NotAvailable) }).Result);


                // --- begin produce, byte[] case.

                Action<DeliveryReport<byte[], byte[]>> dh = (DeliveryReport<byte[], byte[]> dr) => drs2_produce.Add(dr);

                producer.Produce(
                    singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = Timestamp.Default }, dh);

                // TimestampType: CreateTime
                producer.Produce(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)) },
                    dh);

                // TimestampType: CreateTime (default)
                producer.Produce(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = Timestamp.Default }, dh);

                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() =>
                    producer.Produce(
                        singlePartitionTopic,
                        new Message<byte[], byte[]> { Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) }, dh));

                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() =>
                    producer.Produce(singlePartitionTopic,
                    new Message<byte[], byte[]> { Timestamp = new Timestamp(10, TimestampType.NotAvailable) }, dh));

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build())
            {
                // serializing async

                assertCloseToNow(consumer, drs_task[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_task[1].TopicPartitionOffset});
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_task[2].TopicPartitionOffset);

                // serializing deliveryhandler

                assertCloseToNow(consumer, drs_produce[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_produce[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_produce[2].TopicPartitionOffset);
            }

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                ConsumeResult<byte[], byte[]> record;

                // non-serializing async

                assertCloseToNow_byte(consumer, drs2_task[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs2_task[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow_byte(consumer, drs2_task[2].TopicPartitionOffset);

                // non-serializing deliveryhandler

                assertCloseToNow_byte(consumer, drs2_produce[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs2_produce[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow_byte(consumer, drs2_produce[2].TopicPartitionOffset);
            }
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Timestamps");
        }

        private static void assertCloseToNow(IConsumer<Null, string> consumer, TopicPartitionOffset tpo)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            var cr = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(cr.Message);
            Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
            Assert.True(Math.Abs((cr.Message.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }

        private static void assertCloseToNow_byte(IConsumer<byte[], byte[]> consumer, TopicPartitionOffset tpo)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            var cr = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(cr.Message);
            Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
            Assert.True(Math.Abs((cr.Message.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }
    }
}
