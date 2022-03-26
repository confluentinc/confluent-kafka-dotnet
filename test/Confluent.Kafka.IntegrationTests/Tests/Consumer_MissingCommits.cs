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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test consumer behavior in a scenario where some topic partitions have
        ///     committed offsets and others don't.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_MissingCommits(string bootstrapServers)
        {
            LogToFile("start Consumer_MissingCommits");

            var groupId = Guid.NewGuid().ToString();
            var numPartitions = 15;
            var numConsumers = 10;
            var numPartitionsWithCommittedOffsets = 10;
            var numMessagesPerPartition = 10;
            var commitOffset = 8;
            using (var topic = new TemporaryTopic(bootstrapServers, numPartitions))
            {
                using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                using (var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId }).Build())
                {
                    for (int i=0; i<numPartitions; ++i)
                    {
                        for (int j=0; j<numMessagesPerPartition; ++j)
                        {
                            producer.Produce(new TopicPartition(topic.Name, i), new Message<Null, string> { Value = "test" });
                        }
                    }
                    producer.Flush();

                    for (int i=0; i<numPartitionsWithCommittedOffsets; ++i)
                    {
                        consumer.Commit(new TopicPartitionOffset[] { new TopicPartitionOffset(topic.Name, i, commitOffset) });
                    }
                }

                var consumers = new List<IConsumer<Null, string>>();
                var messageCounts = new Dictionary<int, int>();
                try
                {
                    for (int i=0; i<numConsumers; ++i)
                    {
                        var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest, Debug = "all" }).Build();
                        consumers.Add(consumer);
                        consumer.Subscribe(topic.Name);
                    }

                    var startTime = DateTime.UtcNow;
                    var complete = false;
                    while (!complete)
                    {
                        if (DateTime.UtcNow - startTime > TimeSpan.FromSeconds(30))
                        {
                            Assert.False(true, "Timed out waiting for consumption of messages to complete");
                            complete = true;
                        }

                        for (int i=0; i<numConsumers; ++i)
                        {
                            var cr = consumers[i].Consume(0);
                            if (cr != null && cr.Message != null)
                            {
                                if (!messageCounts.ContainsKey(cr.Partition))
                                {
                                    messageCounts.Add(cr.Partition, 0);
                                }
                                messageCounts[cr.Partition] += 1;
                            }
                        }

                        complete = true;
                        for (int i=0; i<numPartitionsWithCommittedOffsets; ++i)
                        {
                            if (!messageCounts.ContainsKey(i) || messageCounts[i] < numMessagesPerPartition-commitOffset)
                            {
                                complete = false;
                                break;
                            }
                        }
                        for (int i=numPartitionsWithCommittedOffsets; i<numPartitions; ++i)
                        {
                            if (!messageCounts.ContainsKey(i) || messageCounts[i] < numMessagesPerPartition)
                            {
                                complete = false;
                                break;
                            }
                        }
                    }
                }
                finally
                {
                    for (int i=0; i<numConsumers; ++i)
                    {
                        try
                        {
                            consumers[i].Close();
                        }
                        catch
                        {
                            Assert.False(true, "Failed to close consumer instance " + i);
                        }
                    }
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_MissingCommits");
        }
    }
}
