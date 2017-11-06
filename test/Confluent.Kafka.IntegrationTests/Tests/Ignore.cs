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

            Message dr;
            using (var producer = new Producer(producerConfig))
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(singlePartitionTopic, null, null).Result;
                producer.ProduceAsync(singlePartitionTopic, null, new byte[] { 1 }).Wait();
                producer.ProduceAsync(singlePartitionTopic, new byte[] { 0 }, null).Wait();
                producer.ProduceAsync(singlePartitionTopic, new byte[] { 42 }, new byte[] { 42, 240 }).Wait();
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new Consumer<Ignore, Ignore>(consumerConfig, null, null))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });

                Message<Ignore, Ignore> msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.Null(msg.Value);
            }

            using (var consumer = new Consumer<Ignore, byte[]>(consumerConfig, null, new ByteArrayDeserializer()))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset.Value + 3) });

                Message<Ignore, byte[]> msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.NotNull(msg.Value);
                Assert.Equal(msg.Value[0], 42);
                Assert.Equal(msg.Value[1], 240);
            }
        }

    }
}
