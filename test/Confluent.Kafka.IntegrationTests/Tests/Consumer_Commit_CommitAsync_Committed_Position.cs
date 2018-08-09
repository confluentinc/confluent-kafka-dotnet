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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Some simple tests for all variants of Commit / CommitAsync.
        ///       (and also Committed and Position)
        ///     We would ideally have these tests for the non-deserializing consumer
        ///       as well, but the serializing consumer implementation falls straight 
        ///       through to this, so such tests would have little value.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Commit_CommitAsync_Committed_Position(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Commit_CommitAsync_Committed_Position");

            const int N = 8;
            const int Partition = 0;

            var messages = ProduceMessages(bootstrapServers, singlePartitionTopic, Partition, N);
            var firstMsgOffset = messages[0].Offset;

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "enable.auto.commit", false }
            };

            var firstMessage = messages[0];
            var lastMessage = messages[N - 1];
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                
                // Test #0 (empty cases)
                consumer.CommitAsync(new List<TopicPartitionOffset>()).Wait(); // should not throw.
                var committed = consumer.CommittedAsync(new List<TopicPartition>(), TimeSpan.FromSeconds(10)).Result;
                Assert.Empty(committed);
                var ps = consumer.Position(new List<TopicPartition>());
                Assert.Empty(ps);
                
                // Test #0.5 (invalid cases)
                ps = consumer.Position(new List<TopicPartition> { new TopicPartition("invalid-topic", 0) });
                Assert.Single(ps);
                Assert.Equal(Offset.Invalid, ps[0].Offset);
                Assert.Equal("invalid-topic", ps[0].Topic);
                Assert.Equal(0, (int)ps[0].Partition);
                Assert.Equal(new TopicPartition("invalid-topic", 0), ps[0].TopicPartition);

                // Test #1
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                var os = consumer.CommitAsync().Result;
                Assert.Equal(firstMsgOffset + 1, os[0].Offset);
                ps = consumer.Position(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) });
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 1, co[0].Offset);
                Assert.Equal(firstMsgOffset + 1, ps[0].Offset);
                
                // Test #2
                var record2 = consumer.Consume(TimeSpan.FromSeconds(10));
                os = consumer.CommitAsync().Result;
                Assert.Equal(firstMsgOffset + 2, os[0].Offset);
                ps = consumer.Position(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) });
                co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 2, ps[0].Offset);
                Assert.Equal(firstMsgOffset + 2, ps[0].Offset);
            }

            // Test #3
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.CommitAsync(new List<TopicPartitionOffset> { new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset + 5) }).Wait();
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 5, co[0].Offset);
            }
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));

                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                var ps = consumer.Position(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) });
                Assert.Equal(firstMsgOffset + 6, ps[0].Offset);
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 5, co[0].Offset);
            }

            // Test #4
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                consumer.CommitAsync(new List<TopicPartitionOffset> { new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset + 3) });
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                var ps = consumer.Position(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) });
                Assert.Equal(firstMsgOffset + 4, ps[0].Offset);
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            // Test #5
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                var record2 = consumer.Consume(TimeSpan.FromSeconds(10));
                var record3 = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.CommitAsync(record3).Wait();
                var record4 = consumer.Consume(TimeSpan.FromSeconds(10));
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, record.Offset);
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            // Test #6
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartitionOffset(singlePartitionTopic, 0, firstMsgOffset));
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                var record2 = consumer.Consume(TimeSpan.FromSeconds(10));
                var record3 = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.CommitAsync(record3).Wait();
                var record4 = consumer.Consume(TimeSpan.FromSeconds(10));
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }
            using (var consumer = new Consumer<string, string>(consumerConfig, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(firstMsgOffset + 3, record.Offset);
                var co = consumer.CommittedAsync(new List<TopicPartition> { new TopicPartition(singlePartitionTopic, 0) }, TimeSpan.FromSeconds(10)).Result;
                Assert.Equal(firstMsgOffset + 3, co[0].Offset);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Commit_CommitAsync_Committed_Position");
        }

    }
}
