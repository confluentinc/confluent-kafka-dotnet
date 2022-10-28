// Copyright 2022 Confluent Inc.
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


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.DeleteConsumerGroupOffsets.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DeleteConsumerGroupOffsets(string bootstrapServers)
        {
            LogToFile("start AdminClient_DeleteConsumerGroupOffsets");

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            using (var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = topic1.Name }).Build())
            {
                var groupId = topic1.Name;
                var offsetToCommit = 10;

                consumer.Assign(new List<TopicPartition>(){new TopicPartition(topic1.Name, 0)});
                consumer.Commit(new List<TopicPartitionOffset>() {new TopicPartitionOffset(topic1.Name, 0, offsetToCommit)}); //commit some offset for consumer

                var committedOffsets = consumer.Committed(TimeSpan.FromSeconds(10));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(offsetToCommit, committedOffsets[0].Offset);

                List<TopicPartitionOffset> topicPartitionToReset = new List<TopicPartitionOffset>() {new TopicPartitionOffset(topic1.Name, 0, -1)}; //offset value is ignored

                var res = adminClient.DeleteConsumerGroupOffsetsAsync(groupId, topicPartitionToReset).Result;
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);

                committedOffsets = consumer.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(-1001, committedOffsets[0].Offset); //offset is unset

                // Consumer is actively subscribed to the topic
                consumer.Subscribe(new List<String>(){topic1.Name});
                consumer.Commit(new List<TopicPartitionOffset>() {new TopicPartitionOffset(topic1.Name, 0, offsetToCommit)}); //commit some offset for consumer

                committedOffsets = consumer.Committed(TimeSpan.FromSeconds(10));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(offsetToCommit, committedOffsets[0].Offset);

                topicPartitionToReset = new List<TopicPartitionOffset>() {new TopicPartitionOffset(topic1.Name, 0, -1)}; //offset value is ignored
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);

                committedOffsets = consumer.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(10, committedOffsets[0].Offset); //offset is unchanged as the consumer is actively subscribed to the topic

                consumer.Unsubscribe();

                // Deleting offset from the topic which has no set offset
                consumer.Assign(new List<TopicPartition>(){new TopicPartition(topic2.Name, 0)});

                var watermarkOffsets = consumer.QueryWatermarkOffsets(new TopicPartition(topic2.Name, 0),TimeSpan.FromSeconds(10));
                Assert.Equal(0, watermarkOffsets.Low);
                Assert.Equal(0, watermarkOffsets.High);

                topicPartitionToReset = new List<TopicPartitionOffset>() {new TopicPartitionOffset(topic2.Name, 0, -1)}; //offset value is ignored
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);

                watermarkOffsets = consumer.QueryWatermarkOffsets(new TopicPartition(topic2.Name, 0),TimeSpan.FromSeconds(10));
                Assert.Equal(0, watermarkOffsets.Low); //offsets are unchaged after the reset
                Assert.Equal(0, watermarkOffsets.High);
            }
            LogToFile("end   AdminClient_DeleteRecords");
        }
    }
}
