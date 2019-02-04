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
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using Xunit;
using Confluent.Kafka.Serdes;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Simple Consumer Pause / Resume test.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Pause_Resume(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Pause_Resume");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            IEnumerable<TopicPartition> assignedPartitions = null;

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                ConsumeResult<byte[], byte[]> record;

                consumer.SetPartitionsAssignedHandler((c, partitions) => {
                    c.Assign(partitions);
                    assignedPartitions = partitions;
                });

                consumer.Subscribe(singlePartitionTopic);

                while (assignedPartitions == null)
                {
                    consumer.Consume(TimeSpan.FromSeconds(10));
                }
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record);

                producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test value", true, null, null) }).Wait();
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record?.Message);

                consumer.Pause(assignedPartitions);
                producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test value 2", true, null, null) }).Wait();
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Null(record);
                consumer.Resume(assignedPartitions);
                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record?.Message);

                // check that these don't throw.
                consumer.Pause(new List<TopicPartition>());
                consumer.Resume(new List<TopicPartition>());

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Pause_Resume");
        }

    }
}
