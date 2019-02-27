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

using Confluent.Kafka.Serdes;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Simple Consumer StoreOffsets test.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_StoreOffsets(string bootstrapServers, string topic, string partitionedTopic)
        {
            LogToFile("start Consumer_StoreOffsets");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };

            var producerConfig = new ProducerConfig{ BootstrapServers = bootstrapServers };

            IEnumerable<TopicPartition> assignment = null;

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            using (var consumer =
                new ConsumerBuilder<Null, string>(consumerConfig)
                    .SetRebalanceHandler((c, e) =>
                    {
                        if (e.IsAssignment)
                        {
                            c.Assign(e.Partitions);
                            assignment = e.Partitions;
                        }
                    })
                    .Build())
            {
                consumer.Subscribe(topic);

                while (assignment == null)
                {
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }

                ConsumeResult<Null, string> record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record);

                producer.ProduceAsync(topic, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test store offset value", SerializationContext.Empty) }).Wait();
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record?.Message);

                // test doesn't throw.
                consumer.StoreOffset(record);

                // test doesn't throw.
                consumer.StoreOffsets(new List<TopicPartitionOffset>());

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_StoreOffsets");
        }

    }
}
