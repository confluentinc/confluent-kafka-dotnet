// Copyright 2018 Confluent Inc.
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

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///    Some simple tests for all variants of Commit (and also Committed and Position)
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Commit_Committed_Position(string bootstrapServers)
        {
            LogToFile("start Consumer_Commit_Committed_Position");

            const int N = 8;
            const int Partition = 0;

            var messages = ProduceMessages(bootstrapServers, singlePartitionTopic, Partition, N);
            var firstMsgOffset = messages[0].Offset;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false
            };

            var firstMessage = messages[0];
            var lastMessage = messages[N - 1];
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                
                // Test #0.5 (invalid cases)
                var offset = consumer.Position(new TopicPartition("invalid-topic", 0));
                Assert.Equal(Offset.Unset, offset);

                // Test #1
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var os = consumer.Commit();
                Assert.Equal(firstMsgOffset + 1, os[0].Offset);
                offset = consumer.Position(new TopicPartition(singlePartitionTopic, 0));
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 1, co[0].Offset);
                Assert.Equal(firstMsgOffset + 1, offset);
                
                // Test #2
                var record2 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                os = consumer.Commit();
                Assert.Equal(firstMsgOffset + 2, os[0].Offset);
                offset = consumer.Position(new TopicPartition(singlePartitionTopic, 0));
                co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 2, offset);
                Assert.Equal(firstMsgOffset + 2, offset);
            }

            // Test #3
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset + 5) });
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 5, co[0].Offset);
            }
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));

                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var offset = consumer.Position(new TopicPartition(singlePartitionTopic, 0));
                Assert.Equal(firstMsgOffset + 6, offset);
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 5, co[0].Offset);
            }

            // Test #4
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                consumer.Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset + 3) });
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var offset = consumer.Position(new TopicPartition(singlePartitionTopic, 0));
                Assert.Equal(firstMsgOffset + 4, offset);
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            // Test #5
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var record2 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var record3 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                consumer.Commit(record3);
                var record4 = consumer.Consume(TimeSpan.FromMilliseconds(1000));
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                Assert.Equal(firstMsgOffset + 3, record.Offset);
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            // Test #6
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var record2 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var record3 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                consumer.Commit(record3);
                var record4 = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromMilliseconds(6000));
                Assert.Equal(firstMsgOffset + 3, record.Offset);
                var co = consumer.Committed(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Commit_Committed_Position");
        }

    }
}
