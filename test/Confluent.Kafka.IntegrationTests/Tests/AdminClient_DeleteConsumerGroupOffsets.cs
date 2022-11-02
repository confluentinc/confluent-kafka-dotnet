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
using System.Linq;


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
            var assignmentDone = false;

            using (var topic1 = new TemporaryTopic(bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 1))
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            using (var consumer1 = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = topic1.Name })
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    assignmentDone = true;
                    Assert.Equal(1, partitions.Count());
                    Assert.Equal(0, partitions[0].Partition.Value);
                    Assert.Equal(topic1.Name, partitions[0].Topic);
                }).Build())
            {
                var groupId1 = topic1.Name;
                var offsetToCommit = 10;

                consumer1.Assign(new List<TopicPartition>() { new TopicPartition(topic1.Name, 0) });
                consumer1.Commit(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic1.Name, 0, offsetToCommit) }); //commit some offset for consumer

                var committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(10));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(offsetToCommit, committedOffsets[0].Offset);

                List<TopicPartition> topicPartitionToReset = new List<TopicPartition>() { new TopicPartition(topic1.Name, 0) };
                var res = adminClient.DeleteConsumerGroupOffsetsAsync(groupId1, topicPartitionToReset).Result;
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId1, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);
                Assert.Equal(ErrorCode.NoError, res[0].Partitions[0].Error.Code);

                committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(Offset.Unset, committedOffsets[0].Offset);

                // Consumer is actively subscribed to the topic
                assignmentDone = false;
                consumer1.Subscribe(new List<String>() { topic1.Name });
                while (!assignmentDone)
                {
                    // To handle the rebalance
                    consumer1.Consume(TimeSpan.FromSeconds(1));
                }
                consumer1.Commit(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic1.Name, 0, offsetToCommit) }); //commit some offset for consumer

                committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(10));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(offsetToCommit, committedOffsets[0].Offset);

                topicPartitionToReset = new List<TopicPartition>() { new TopicPartition(topic1.Name, 0) };
                res = adminClient.DeleteConsumerGroupOffsetsAsync(groupId1, topicPartitionToReset).Result;
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId1, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);
                Assert.Equal(ErrorCode.GroupSubscribedToTopic, res[0].Partitions[0].Error.Code); // error expected as the consumer is actively subscribed to the topic

                committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(offsetToCommit, committedOffsets[0].Offset); // offset is unchanged as the consumer is actively  ̰subscribed to the topic

                consumer1.Unsubscribe();

                // Deleting offset from the topic which has no set offset
                consumer1.Assign(new List<TopicPartition>() { new TopicPartition(topic2.Name, 0) });

                committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(Offset.Unset, committedOffsets[0].Offset);

                topicPartitionToReset = new List<TopicPartition>() { new TopicPartition(topic2.Name, 0) };
                res = adminClient.DeleteConsumerGroupOffsetsAsync(groupId1, topicPartitionToReset).Result;
                Assert.Equal(1, res.Count);
                Assert.Equal(groupId1, res[0].Group);
                Assert.Equal(1, res[0].Partitions.Count);
                Assert.Equal(0, res[0].Partitions[0].Partition.Value);
                Assert.Equal(ErrorCode.NoError, res[0].Partitions[0].Error.Code);

                committedOffsets = consumer1.Committed(TimeSpan.FromSeconds(1));
                Assert.Equal(1, committedOffsets.Count);
                Assert.Equal(Offset.Unset, committedOffsets[0].Offset); // offsets are unchaged after the reset
            }
            LogToFile("end   AdminClient_DeleteRecords");
        }
    }
}
