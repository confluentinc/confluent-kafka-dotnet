﻿// Copyright 2016-2017 Confluent Inc.
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
                AutoOffsetReset = AutoOffsetResetType.Latest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };

            var producerConfig = new ProducerConfig{ BootstrapServers = bootstrapServers };

            using (var producer = new Producer(producerConfig))
            using (var consumer = new Consumer<Null, string>(consumerConfig))
            {
                IEnumerable<TopicPartition> assignedPartitions = null;
                ConsumeResult<Null, string> record;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    consumer.Assign(partitions);
                    assignedPartitions = partitions;
                };

                consumer.Subscribe(topic);

                while (assignedPartitions == null)
                {
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }

                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record);

                producer.ProduceAsync(topic, new Message { Value = Serializers.Utf8.Serialize("test store offset value", true, null, null) }).Wait();
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
