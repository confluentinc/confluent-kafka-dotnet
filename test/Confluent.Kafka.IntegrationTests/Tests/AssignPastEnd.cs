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
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test functionality of Consumer.Consume when assigned to offest
        ///     higher than the offset of the last message on a partition.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AssignPastEnd(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            DeliveryReport<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = testString }).Result;
                Assert.True(dr.Offset >= 0);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            consumerConfig["auto.offset.reset"] = "latest";
            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
            {
                ConsumeResult<byte[], byte[]> record;

                // Consume API
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+1) });
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record.Message);
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+2) });
                consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record.Message);

                consumer.Close();
            }

            consumerConfig["auto.offset.reset"] = "earliest";
            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
            {
                ConsumeResult<byte[], byte[]> record;
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+1) });
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record.Message);
                // Note: dr.Offset+2 is an invalid (c.f. dr.Offset+1 which is valid), so auto.offset.reset will come
                // into play here to determine which offset to start from (earliest). Due to the the produce call above,
                // there is guarenteed to be a message on the topic, so consumer.Consume will return true.
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset+2) });
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);

                consumer.Close();
            }
        }

    }
}
