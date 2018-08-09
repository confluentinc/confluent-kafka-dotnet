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
        ///     Test various message header produce / consume scenarios.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void MessageHeaderProduceConsume(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start MessageHeaderProduceConsume");

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "max.in.flight", 1 }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            var drs = new List<DeliveryReport<Null, string>>();
            DeliveryReport<Null, string> dr_single, dr_empty, dr_null, dr_multiple, dr_duplicate;
            DeliveryReport<Null, string> dr_ol1, dr_ol3;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                // single header value.
                var headers = new Headers();
                headers.Add("test-header", new byte[] { 142 } );
                dr_single = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value", Headers = headers }).Result;
                Assert.Single(dr_single.Message.Headers);
                Assert.Equal("test-header", dr_single.Message.Headers[0].Key);
                Assert.Equal(new byte[] { 142 }, dr_single.Message.Headers[0].Value);

                // empty header values
                var headers0 = new Headers();
                dr_empty = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value", Headers = headers0 }).Result;
                Assert.Empty(dr_empty.Message.Headers);

                // null header value
                dr_null = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value" }).Result;
                Assert.Empty(dr_null.Message.Headers);

                // multiple header values (also Headers no Dictionary, since order is tested).
                var headers2 = new Headers();
                headers2.Add("test-header-a", new byte[] { 111 } );
                headers2.Add("test-header-b", new byte[] { 112 } );
                dr_multiple = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value", Headers = headers2 }).Result;
                Assert.Equal(2, dr_multiple.Message.Headers.Count);
                Assert.Equal("test-header-a", dr_multiple.Message.Headers[0].Key);
                Assert.Equal(new byte[] { 111 }, dr_multiple.Message.Headers[0].Value);
                Assert.Equal("test-header-b", dr_multiple.Message.Headers[1].Key);
                Assert.Equal(new byte[] { 112 }, dr_multiple.Message.Headers[1].Value);

                // duplicate header values (also List not Dictionary)
                var headers3 = new Headers();
                headers3.Add(new Header("test-header-a", new byte[] { 111 } ));
                headers3.Add(new Header("test-header-b", new byte[] { 112 } ));
                headers3.Add(new Header("test-header-a", new byte[] { 113 } ));
                headers3.Add(new Header("test-header-b", new byte[] { 114 } ));
                headers3.Add(new Header("test-header-c", new byte[] { 115 } ));
                dr_duplicate = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value", Headers = headers3 }).Result;
                Assert.Equal(5, dr_duplicate.Message.Headers.Count);
                Assert.Equal("test-header-a", dr_duplicate.Message.Headers[0].Key);
                Assert.Equal(new byte[] { 111 }, dr_duplicate.Message.Headers[0].Value);
                Assert.Equal("test-header-a", dr_duplicate.Message.Headers[2].Key);
                Assert.Equal(new byte[] { 113 }, dr_duplicate.Message.Headers[2].Value);

                // Test headers work as expected with all serializing ProduceAsync variants.

                dr_ol1 = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "the value" }).Result;
                Assert.Empty(dr_ol1.Message.Headers);
                dr_ol3 = producer.ProduceAsync(
                    new TopicPartition(singlePartitionTopic, 0),
                    new Message<Null, string> { Value = "the value", Headers = headers }
                ).Result;
                Assert.Single(dr_ol3.Message.Headers);
                Assert.Equal("test-header", dr_ol3.Message.Headers[0].Key);
                Assert.Equal(new byte[] { 142 }, dr_ol3.Message.Headers[0].Value);

                Action<DeliveryReport<Null, string>> dh = (DeliveryReport<Null, string> dr) => drs.Add(dr);

                // Test headers work as expected with all serializing Produce variants. 

                producer.BeginProduce(singlePartitionTopic, new Message<Null, string> { Value = "the value" }, dh);
                producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0), 
                    new Message<Null, string> { Value = "the value", Headers = headers2},
                    dh);

                producer.Flush(TimeSpan.FromSeconds(10));

                Assert.Empty(drs[0].Message.Headers);
                Assert.Equal(2, drs[1].Message.Headers.Count);
            }

            List<DeliveryReport<byte[], byte[]>> drs_2 = new List<DeliveryReport<byte[], byte[]>>();
            DeliveryReport<byte[], byte[]> dr_ol4, dr_ol5, dr_ol6, dr_ol7;
            using (var producer = new Producer<byte[], byte[]>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer()))
            {
                var headers = new Headers();
                headers.Add("hkey", new byte[] { 44 });

                // Test headers work as expected with all non-serializing ProduceAsync variants. 

                dr_ol4 = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Headers = null }).Result;
                Assert.Empty(dr_ol4.Message.Headers);
                dr_ol5 = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Headers = null }).Result;
                Assert.Empty(dr_ol5.Message.Headers);
                dr_ol6 = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Headers = headers }).Result;
                Assert.Single(dr_ol6.Message.Headers);
                dr_ol7 = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Headers = headers }).Result;
                Assert.Single(dr_ol7.Message.Headers);

                // Test headers work as expected with all non-serializing Produce variants.

                Action<DeliveryReport<byte[], byte[]>> dh = (DeliveryReport<byte[], byte[]> dr) => drs_2.Add(dr);

                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Headers = headers }, dh);
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Headers = null }, dh);
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Headers = headers }, dh);
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Headers = headers }, dh);

                producer.Flush(TimeSpan.FromSeconds(10));

                Assert.Single(drs_2[0].Message.Headers);
                Assert.Empty(drs_2[1].Message.Headers); // TODO: this is intermittently not working.
                Assert.Single(drs_2[2].Message.Headers);
                Assert.Single(drs_2[3].Message.Headers);
            }

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
            {
                consumer.Assign(new List<TopicPartitionOffset>() {dr_single.TopicPartitionOffset});
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Single(record.Message.Headers);
                Assert.Equal("test-header", record.Message.Headers[0].Key);
                Assert.Equal(new byte[] { 142 }, record.Message.Headers[0].Value);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_empty.TopicPartitionOffset});
                var record2 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record2.Message);
                // following Java, alway instantiate a new Headers instance, even in the empty case.
                Assert.NotNull(record2.Message.Headers);
                Assert.Empty(record2.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_null.TopicPartitionOffset});
                var record3 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record3.Message);
                Assert.NotNull(record3.Message.Headers);
                Assert.Empty(record3.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_multiple.TopicPartitionOffset});
                var record4 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record4.Message);
                Assert.Equal(2, record4.Message.Headers.Count);
                Assert.Equal("test-header-a", record4.Message.Headers[0].Key);
                Assert.Equal("test-header-b", record4.Message.Headers[1].Key);
                Assert.Equal(new byte[] { 111 }, record4.Message.Headers[0].Value);
                Assert.Equal(new byte[] { 112 }, record4.Message.Headers[1].Value);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_duplicate.TopicPartitionOffset});
                var record5 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record5.Message);
                Assert.Equal(5, record5.Message.Headers.Count);
                Assert.Equal("test-header-a", record5.Message.Headers[0].Key);
                Assert.Equal("test-header-b", record5.Message.Headers[1].Key);
                Assert.Equal("test-header-a", record5.Message.Headers[2].Key);
                Assert.Equal("test-header-b", record5.Message.Headers[3].Key);
                Assert.Equal("test-header-c", record5.Message.Headers[4].Key);
                Assert.Equal(new byte[] { 111 }, record5.Message.Headers[0].Value);
                Assert.Equal(new byte[] { 112 }, record5.Message.Headers[1].Value);
                Assert.Equal(new byte[] { 113 }, record5.Message.Headers[2].Value);
                Assert.Equal(new byte[] { 114 }, record5.Message.Headers[3].Value);
                Assert.Equal(new byte[] { 115 }, record5.Message.Headers[4].Value);
                Assert.Equal(new byte[] { 113 }, record5.Message.Headers.GetLast("test-header-a"));
                Assert.Equal(new byte[] { 114 }, record5.Message.Headers.GetLast("test-header-b"));
                Assert.Equal(new byte[] { 115 }, record5.Message.Headers.GetLast("test-header-c"));

                // Test headers work with all produce method variants.

                // async, serializing
                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol1.TopicPartitionOffset});
                var record6 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record6.Message);
                Assert.Empty(record6.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol3.TopicPartitionOffset});
                var record8 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record8.Message);
                Assert.Single(record8.Message.Headers);

                // delivery-handler, serializing.
                consumer.Assign(new List<TopicPartitionOffset>() {drs[0].TopicPartitionOffset});
                var record9 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record9.Message);
                Assert.Empty(record9.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {drs[1].TopicPartitionOffset});
                var record11 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record11.Message);
                Assert.Equal(2, record11.Message.Headers.Count);

                // async, non-serializing
                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol4.TopicPartitionOffset});
                var record12 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record12.Message);
                Assert.Empty(record12.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol5.TopicPartitionOffset});
                var record13 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record13.Message);
                Assert.Empty(record13.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol6.TopicPartitionOffset});
                var record14 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record14.Message);
                Assert.Single(record14.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {dr_ol7.TopicPartitionOffset});
                var record15 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record15.Message);
                Assert.Single(record15.Message.Headers);

                // delivery handler, non-serializing
                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[0].TopicPartitionOffset});
                var record16 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record16.Message);
                Assert.Single(record16.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[1].TopicPartitionOffset});
                var record17 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record17.Message);
                Assert.Empty(record17.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[2].TopicPartitionOffset});
                var record18 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record18.Message);
                Assert.Single(record18.Message.Headers);

                consumer.Assign(new List<TopicPartitionOffset>() {drs_2[3].TopicPartitionOffset});
                var record19 = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record19.Message);
                Assert.Single(record19.Message.Headers);
            }

            // null key
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                var headers = new Headers();
                var threw = false;
                try
                {
                    headers.Add(null, new byte[] { 142 } );
                }
                catch
                {
                    threw = true;
                }
                finally
                {
                    Assert.True(threw);
                }

                var headers2 = new List<Header>();
                Assert.Throws<ArgumentNullException>(() => headers2.Add(new Header(null, new byte[] { 42 })));
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   MessageHeaderProduceConsume");
        }
    }
}
