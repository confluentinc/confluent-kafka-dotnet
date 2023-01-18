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

#pragma warning disable xUnit1026

using System;
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.ListConsumerGroupOffsets and
        ///     AdminClient.AlterConsumerGroupOffsets.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_AlterListConsumerGroupOffsets(string bootstrapServers)
        {
            LogToFile("start AdminClient_AlterListConsumerGroupOffsets");
            var numMessages = 5;
            var groupID = Guid.NewGuid().ToString();

            using(var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                // This test needs us to first produce and consume from a topic before we can list the offsets.
                // 1. Create topic and produce
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    EnableIdempotence = true,
                    LingerMs = 1.5
                };

                using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
                {
                    for (int i = 0; i < numMessages; i++)
                    {
                        producer.Produce(
                            new TopicPartition(topic.Name, 0),
                            new Message<string, string> { Key = "test key " + i, Value = "test val " + i });

                    }
                    producer.Flush(TimeSpan.FromSeconds(10));
                }


                // Create an AdminClient here - to test alter while the consumer is still active.
                var adminClient = new AdminClientBuilder(new AdminClientConfig {
                    BootstrapServers = bootstrapServers,
                }).Build();

                // 2. Consume
                var consumerConfig = new ConsumerConfig
                {
                    GroupId = groupID,
                    BootstrapServers = bootstrapServers,
                    SessionTimeoutMs = 6000,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                    EnablePartitionEof = true,
                };

                using (var consumer =
                    new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Subscribe(topic.Name);

                    int msgCnt = 0;
                    while (true)
                    {
                        var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (record == null) { continue; }
                        if (record.IsPartitionEOF) { break; }
                        msgCnt += 1;
                        consumer.StoreOffset(record);
                    }

                    Assert.Equal(numMessages, msgCnt);
                    consumer.Commit();

                    // Check that we are not be able to alter the offsets while the consumer is still active.
                    var errorOccured = false;
                    try
                    {
                        var tpoListInvalid = new List<TopicPartitionOffset>();
                        tpoListInvalid.Add(new TopicPartitionOffset(topic.Name, 0, 2));
                        var _ = adminClient.AlterConsumerGroupOffsetsAsync(
                            new ConsumerGroupTopicPartitionOffsets[] {
                                new ConsumerGroupTopicPartitionOffsets(groupID, tpoListInvalid),
                        }).Result;
                    }
                    catch (Exception e)
                    {
                        errorOccured = true;
                        Assert.IsType<AlterConsumerGroupOffsetsException>(e.InnerException);
                    }
                    Assert.True(errorOccured);

                    consumer.Close();
                }

                // 3. List, Alter and then again List Consumer Group Offsets
                var tpList = new List<TopicPartition>();
                tpList.Add(new TopicPartition(topic.Name, 0));
                var lcgoResults = adminClient.ListConsumerGroupOffsetsAsync(
                    new ConsumerGroupTopicPartitions[] {
                        new ConsumerGroupTopicPartitions(groupID, tpList),
                    },
                    new ListConsumerGroupOffsetsOptions() { RequireStableOffsets = false }
                ).Result;

                Assert.Single(lcgoResults);

                var groupResultListing = lcgoResults[0];
                Assert.NotNull(groupResultListing);
                Assert.Single(groupResultListing.Partitions);
                Assert.Equal(topic.Name, groupResultListing.Partitions[0].Topic);
                Assert.Equal(0, groupResultListing.Partitions[0].Partition.Value);
                Assert.Equal(5, groupResultListing.Partitions[0].Offset);
                Assert.False(groupResultListing.Partitions[0].Error.IsError);

                var tpoList = new List<TopicPartitionOffset>();
                tpoList.Add(new TopicPartitionOffset(topic.Name, 0, 2));
                var acgoResults = adminClient.AlterConsumerGroupOffsetsAsync(
                    new ConsumerGroupTopicPartitionOffsets[] {
                        new ConsumerGroupTopicPartitionOffsets(groupID, tpoList),
                }).Result;

                Assert.Single(acgoResults);
                var groupResultAlter = acgoResults[0];
                Assert.NotNull(groupResultAlter);
                Assert.Single(groupResultAlter.Partitions);
                Assert.Equal(topic.Name, groupResultAlter.Partitions[0].Topic);
                Assert.Equal(0, groupResultAlter.Partitions[0].Partition.Value);
                Assert.Equal(2, groupResultAlter.Partitions[0].Offset);
                Assert.False(groupResultAlter.Partitions[0].Error.IsError);

                tpList = new List<TopicPartition>();
                tpList.Add(new TopicPartition(topic.Name, 0));
                lcgoResults = adminClient.ListConsumerGroupOffsetsAsync(
                    new ConsumerGroupTopicPartitions[] {
                        new ConsumerGroupTopicPartitions(groupID, tpList),
                    },
                    new ListConsumerGroupOffsetsOptions() { RequireStableOffsets = false }
                ).Result;

                Assert.Single(lcgoResults);

                groupResultListing = lcgoResults[0];
                Assert.NotNull(groupResultListing);
                Assert.Single(groupResultListing.Partitions);
                Assert.Equal(topic.Name, groupResultListing.Partitions[0].Topic);
                Assert.Equal(0, groupResultListing.Partitions[0].Partition.Value);
                Assert.Equal(2, groupResultListing.Partitions[0].Offset);
                Assert.False(groupResultListing.Partitions[0].Error.IsError);

                adminClient.Dispose();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_AlterListConsumerGroupOffsets");
        }
    }
}
