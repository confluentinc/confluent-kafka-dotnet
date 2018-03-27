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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that the ignore deserialier behaves as expected.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void IgnoreTest(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers }
            };

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            DeliveryReport dr;
            using (var producer = new Producer(producerConfig))
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(singlePartitionTopic, 0, null, 0, 0, null, 0, 0, Timestamp.Default, new Headers()).Result;
                producer.ProduceAsync(singlePartitionTopic, Partition.Any, null, 0, 0, new byte[] { 1 }, 0, 1, Timestamp.Default, null).Wait();
                producer.ProduceAsync(singlePartitionTopic, Partition.Any, new byte[] { 0 }, 0, 1, null, 0, 0, Timestamp.Default, null).Wait();
                producer.ProduceAsync(singlePartitionTopic, Partition.Any, new byte[] { 42 }, 0, 1, new byte[] { 42, 240 }, 0, 2, Timestamp.Default, null).Wait();
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new Consumer<Ignore, Ignore>(consumerConfig, null, null))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });

                ConsumerRecord<Ignore, Ignore> record;
                Assert.True(consumer.Consume(out record, TimeSpan.FromMinutes(1)));
                Assert.NotNull(record);
                Assert.Null(record.Message.Key);
                Assert.Null(record.Message.Value);

                Assert.True(consumer.Consume(out record, TimeSpan.FromMinutes(1)));
                Assert.NotNull(record);
                Assert.Null(record.Message.Key);
                Assert.Null(record.Message.Value);

                Assert.True(consumer.Consume(out record, TimeSpan.FromMinutes(1)));
                Assert.NotNull(record);
                Assert.Null(record.Message.Key);
                Assert.Null(record.Message.Value);
            }

            using (var consumer = new Consumer<Ignore, byte[]>(consumerConfig, null, new ByteArrayDeserializer()))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset.Value + 3) });

                ConsumerRecord<Ignore, byte[]> record;
                Assert.True(consumer.Consume(out record, TimeSpan.FromMinutes(1)));
                Assert.NotNull(record);
                Assert.Null(record.Message.Key);
                Assert.NotNull(record.Message.Value);
                Assert.Equal(42, record.Message.Value[0]);
                Assert.Equal(240, record.Message.Value[1]);
            }
        }

    }
}
