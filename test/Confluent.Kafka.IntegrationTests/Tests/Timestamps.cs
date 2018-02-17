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
        class DeliveryHandler_TCB : IDeliveryHandler<Null, string>
        {
            public static List<Message<Null, string>> drs 
                = new List<Message<Null, string>>();

            public bool MarshalData
                => true;

            public void HandleDeliveryReport(Message<Null, string> message)
            {
                drs.Add(message);
            }
        }

        class DeliveryHandler_TCB_2 : IDeliveryHandler
        {
            public static List<Message> drs 
                = new List<Message>();

            public bool MarshalData
                => true;

            public void HandleDeliveryReport(Message message)
            {
                drs.Add(message);
            }
        }

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

            List<Message<Null, string>> drs = new List<Message<Null, string>>();
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                drs.Add(producer.ProduceAsync(singlePartitionTopic, null, "testvalue").Result);
                
                // Note: in the Message<K,V> case, timestamps of type LogAppendTime and NotAvailable are not considered errors 
                // since normal use case is for re-trying failed messages. They are silently changed to Timestamp.Default.

                // TimestampType: CreateTime
                drs.Add(producer.ProduceAsync(new Message<Null, string>(singlePartitionTopic, 0, 5233, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null, null)).Result);
                // TimestampType: CreateTime (default)
                drs.Add(producer.ProduceAsync(new Message<Null, string>(singlePartitionTopic, 0, 342, null, "test-value", Timestamp.Default, null, null)).Result);
                // TimestampType: LogAppendTime
                drs.Add(producer.ProduceAsync(new Message<Null, string>(singlePartitionTopic, 0, 111, null, "tst", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null, null)).Result);
                // TimestampType: NotAvailable
                drs.Add(producer.ProduceAsync(new Message<Null, string>(singlePartitionTopic, 0, 111, null, "tst", new Timestamp(0, TimestampType.NotAvailable), null, null)).Result);

                // TimestampType: CreateTime
                drs.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null).Result); 
                // TimestampType: CreateTime (default)
                drs.Add(producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", Timestamp.Default, null).Result);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null).Result);
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.ProduceAsync(singlePartitionTopic, 0, null, "test-value", new Timestamp(10, TimestampType.NotAvailable), null).Result);


                var dh = new DeliveryHandler_TCB();

                producer.Produce(singlePartitionTopic, null, "testvalue", dh);
                
                // TimestampType: CreateTime
                producer.Produce(new Message<Null, string>(singlePartitionTopic, 0, 5233, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null, null), dh);
                // TimestampType: CreateTime (default)
                producer.Produce(new Message<Null, string>(singlePartitionTopic, 0, 342, null, "test-value", Timestamp.Default, null, null), dh);
                // TimestampType: LogAppendTime
                producer.Produce(new Message<Null, string>(singlePartitionTopic, 0, 111, null, "tst", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null, null), dh);
                // TimestampType: NotAvailable
                producer.Produce(new Message<Null, string>(singlePartitionTopic, 0, 111, null, "tst", new Timestamp(0, TimestampType.NotAvailable), null, null), dh);

                // TimestampType: CreateTime
                producer.Produce(singlePartitionTopic, 0, null, "test-value", new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null, dh);
                // TimestampType: CreateTime (default)
                producer.Produce(singlePartitionTopic, 0, null, "test-value", Timestamp.Default, null, dh);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.Produce(singlePartitionTopic, 0, null, "test-value", new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null, dh));
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.Produce(singlePartitionTopic, 0, null, "test-value", new Timestamp(10, TimestampType.NotAvailable), null, dh));
            }

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

                var dh = new DeliveryHandler_TCB_2();

                producer.Produce(singlePartitionTopic, Partition.Any, null, 0, 0, null, 0, 0, Timestamp.Default, null, dh);

                // TimestampType: CreateTime
                producer.Produce(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)), null, dh);
                // TimestampType: CreateTime (default)
                producer.Produce(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, Timestamp.Default, null, dh);
                // TimestampType: LogAppendTime
                Assert.Throws<ArgumentException>(() => producer.Produce(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(DateTime.Now, TimestampType.LogAppendTime), null, dh));
                // TimestampType: NotAvailable
                Assert.Throws<ArgumentException>(() => producer.Produce(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, new Timestamp(10, TimestampType.NotAvailable), null, dh));
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
                assertCloseToNow(consumer, drs[3].TopicPartitionOffset);
                assertCloseToNow(consumer, drs[4].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[5].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs[6].TopicPartitionOffset);

                // serializing ideliveryhandler

                assertCloseToNow(consumer, DeliveryHandler_TCB.drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {DeliveryHandler_TCB.drs[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, DeliveryHandler_TCB.drs[2].TopicPartitionOffset);
                assertCloseToNow(consumer, DeliveryHandler_TCB.drs[3].TopicPartitionOffset);
                assertCloseToNow(consumer, DeliveryHandler_TCB.drs[4].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {DeliveryHandler_TCB.drs[5].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, DeliveryHandler_TCB.drs[6].TopicPartitionOffset);

                // non-serializing async

                assertCloseToNow(consumer, drs2[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {drs2[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, drs2[2].TopicPartitionOffset);


                // non-serializing ideliveryhandler

                assertCloseToNow(consumer, DeliveryHandler_TCB_2.drs[0].TopicPartitionOffset);

                consumer.Assign(new List<TopicPartitionOffset>() {DeliveryHandler_TCB_2.drs[1].TopicPartitionOffset});
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
                Assert.Equal(msg.Timestamp, new Timestamp(new DateTime(2008, 11, 12, 0, 0, 0, DateTimeKind.Utc)));

                assertCloseToNow(consumer, DeliveryHandler_TCB_2.drs[2].TopicPartitionOffset);
            }
        }
        private static void assertCloseToNow(Consumer consumer, TopicPartitionOffset tpo)
        {
            Message msg;
            consumer.Assign(new List<TopicPartitionOffset>() {tpo});
            Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
            Assert.Equal(TimestampType.CreateTime, msg.Timestamp.Type);
            Assert.True( Math.Abs((msg.Timestamp.UtcDateTime - DateTime.UtcNow).TotalSeconds) < 120);
        }

    }
}
