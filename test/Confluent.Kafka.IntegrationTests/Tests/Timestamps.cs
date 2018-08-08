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
using System.Text;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Integration tests for Producing / consuming timestamps.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void CustomTimestampTests(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start CustomTimestampTests");

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            var drs_1 = new List<DeliveryReport<Null, string>>();
            List<DeliveryReport<Null, string>> drs = new List<DeliveryReport<Null, string>>();
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                drs.Add(producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "testvalue" }).Result);
                
                // TimestampType: CreateTime
                drs.Add(producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc))
                    }
                ).Result); 

                // TimestampType: CreateTime (default)
                drs.Add(producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> { Value = "test-value" }).Result);

                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) 
                    }
                ).Result);

                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value",
                        Timestamp = new Timestamp(10, TimestampType.NotAvailable)
                    }
                ).Result);

                Action<DeliveryReport<Null, string>> dh 
                    = (DeliveryReport<Null, string> dr) => drs_1.Add(dr);

                producer.BeginProduce(singlePartitionTopic, new Message<Null, string> { Value = "testvalue" }, dh);

                // TimestampType: CreateTime
                producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc))
                    }, dh);

                // TimestampType: CreateTime (default)
                producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> { Value = "test-value" },
                    dh
                );

                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime)
                    }, 
                    dh
                ));

                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> 
                    { 
                        Value = "test-value", 
                        Timestamp = new Timestamp(10, TimestampType.NotAvailable)
                    },
                    dh
                ));

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            var drs_2 = new List<DeliveryReport<byte[], byte[]>>();
            List<DeliveryReport<byte[], byte[]>> drs2 = new List<DeliveryReport<byte[], byte[]>>();
            using (var producer = new Producer<byte[], byte[]>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer()))
            {
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = Timestamp.Default }).Result);

                // TimestampType: CreateTime
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)) }).Result);
                // TimestampType: CreateTime (default)
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = Timestamp.Default }).Result);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) }).Result);
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(10, TimestampType.NotAvailable) }).Result);

                Action<DeliveryReport<byte[], byte[]>> dh = (DeliveryReport<byte[], byte[]> dr) => drs_2.Add(dr);

                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = Timestamp.Default }, dh);

                // TimestampType: CreateTime
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)) }, dh);
                // TimestampType: CreateTime (default)
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = Timestamp.Default }, dh);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(DateTime.Now, TimestampType.LogAppendTime) }, dh));
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Timestamp = new Timestamp(10, TimestampType.NotAvailable) }, dh));

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
            {
                // serializing async

                assertCloseToNow(consumer, drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[1].TopicPartitionOffset});
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs[2].TopicPartitionOffset);

                // serializing deliveryhandler

                assertCloseToNow(consumer, drs_1[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_1[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_1[2].TopicPartitionOffset);

                // non-serializing async

                assertCloseToNow(consumer, drs2[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs2[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs2[2].TopicPartitionOffset);

                // non-serializing deliveryhandler

                assertCloseToNow(consumer, drs_2[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[1].TopicPartitionOffset});
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                Assert.Equal(record.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_2[2].TopicPartitionOffset);
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                ConsumeResult<Null, string> cr;

                // serializing async

                assertCloseToNowTyped(consumer, drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[1].TopicPartitionOffset});
                cr = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr.Message);
                Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
                Assert.Equal(cr.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));
                
                assertCloseToNowTyped(consumer, drs[2].TopicPartitionOffset);

                // serializing deliveryhandler

                assertCloseToNowTyped(consumer, drs_1[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_1[1].TopicPartitionOffset});
                cr = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr.Message);
                Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
                Assert.Equal(cr.Message.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNowTyped(consumer, drs_1[2].TopicPartitionOffset);
            }
            
            LogToFile("end   CustomTimestampTests");
        }

        private static void assertCloseToNowTyped(Consumer<Null, string> consumer, TopicPartitionOffset tpo)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            var cr = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(cr.Message);
            Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
            Assert.True(Math.Abs((cr.Message.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }

        private static void assertCloseToNow(Consumer<byte[], byte[]> consumer, TopicPartitionOffset tpo)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            var cr = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(cr.Message);
            Assert.Equal(TimestampType.CreateTime, cr.Message.Timestamp.Type);
            Assert.True(Math.Abs((cr.Message.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }
    }
}
