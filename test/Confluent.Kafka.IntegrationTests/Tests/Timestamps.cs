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

            var drs_1 = new List<Message<Null, string>>();
            List<Message<Null, string>> drs = new List<Message<Null, string>>();
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                drs.Add(producer.ProduceAsync(singlePartitionTopic, null, "testvalue").Result);
                
                // TimestampType: CreateTime
                drs.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null).Result); 
                // TimestampType: CreateTime (default)
                drs.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", Timestamp.Default, null).Result);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null).Result);
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(10, TimestampType.NotAvailable), null).Result);

                Action<Message<Null, string>> dh = (Message<Null, string> dr) => drs_1.Add(dr);

                producer.Produce(dh, singlePartitionTopic, null, "testvalue");

                // TimestampType: CreateTime
                producer.Produce(dh, singlePartitionTopic, 0, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null);
                // TimestampType: CreateTime (default)
                producer.Produce(dh, singlePartitionTopic, 0, null, "test-value", Timestamp.Default, null);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.Produce(dh, singlePartitionTopic, 0, null, "test-value", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null));
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.Produce(dh, singlePartitionTopic, 0, null, "test-value", new Timestamp(10, TimestampType.NotAvailable), null));
            }

            var drs_2 = new List<Message>();
            List<Message> drs2 = new List<Message>();
            using (var producer = new Producer(producerConfig))
            {
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, Partition.Any, null, 0, 0, null, 0, 0, Timestamp.Default, null).Result);

                // TimestampType: CreateTime
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null).Result);
                // TimestampType: CreateTime (default)
                drs2.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, Timestamp.Default, null).Result);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null).Result);
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(10, TimestampType.NotAvailable), null).Result);

                Action<Message> dh = (Message dr) => drs_2.Add(dr);

                producer.Produce(dh, singlePartitionTopic, Partition.Any, null, 0, 0, null, 0, 0, Timestamp.Default, null);

                // TimestampType: CreateTime
                producer.Produce(dh, singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null);
                // TimestampType: CreateTime (default)
                producer.Produce(dh, singlePartitionTopic, 0, null, 0, 0, null, 0, 0, Timestamp.Default, null);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.Produce(dh, singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null));
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.Produce(dh, singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(10, TimestampType.NotAvailable), null));
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                Message msg;

                // serializing async

                assertCloseToNow(consumer, drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs[2].TopicPartitionOffset);

                // serializing deliveryhandler

                assertCloseToNow(consumer, drs_1[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_1[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_1[2].TopicPartitionOffset);

                // non-serializing async

                assertCloseToNow(consumer, drs2[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs2[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs2[2].TopicPartitionOffset);

                // non-serializing deliveryhandler

                assertCloseToNow(consumer, drs_2[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs_2[2].TopicPartitionOffset);
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                Message<Null, string> msg;

                // serializing async

                assertCloseToNowTyped(consumer, drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));
                
                assertCloseToNowTyped(consumer, drs[2].TopicPartitionOffset);

                // serializing deliveryhandler

                assertCloseToNowTyped(consumer, drs_1[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_1[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNowTyped(consumer, drs_1[2].TopicPartitionOffset);
            }
        }

        private static void assertCloseToNowTyped(Consumer<Null, string> consumer, TopicPartitionOffset tpo)
        {
            Message<Null, string> msg;
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
            Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
            Assert.True(Math.Abs((msg.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }

        private static void assertCloseToNow(Consumer consumer, TopicPartitionOffset tpo)
        {
            Message msg;
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
            Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
            Assert.True(Math.Abs((msg.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }

    }
}
