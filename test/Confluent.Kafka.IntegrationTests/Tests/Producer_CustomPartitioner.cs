// Copyright 2020 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Custom partitioner test.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_CustomPartitioner(string bootstrapServers)
        {
            LogToFile("start Producer_CustomPartitioner");

            const int PARTITION_COUNT = 42;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
            };

            for (int j=0; j<3; ++j)
            {
                using (var topic = new TemporaryTopic(bootstrapServers, PARTITION_COUNT))
                {
                    Action<DeliveryReport<string, string>> dh = (DeliveryReport<string, string> dr) =>
                    {
                        Assert.StartsWith($"test key ", dr.Message.Key);
                        Assert.StartsWith($"test val ", dr.Message.Value);
                        var expectedPartition = int.Parse(dr.Message.Key.Split(" ").Last());
                        Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                        Assert.Equal(PersistenceStatus.Persisted, dr.Status);
                        Assert.Equal(topic.Name, dr.Topic);
                        Assert.Equal(expectedPartition, (int)dr.Partition);
                        Assert.True(dr.Offset >= 0);
                        Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                        Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                    };

                    ProducerBuilder<string, string> producerBuilder = null;
                    switch (j)
                    {
                        case 0:
                            // Topic level custom partitioner.
                            producerBuilder = new ProducerBuilder<string, string>(producerConfig);
                            producerBuilder.SetPartitioner(topic.Name, (string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                            {
                                Assert.Equal(topic.Name, topicName);
                                var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                                return int.Parse(keyString.Split(" ").Last()) % partitionCount;
                            });
                            break;
                        case 1:
                            // Default custom partitioner
                            producerBuilder = new ProducerBuilder<string, string>(producerConfig);
                            producerBuilder.SetDefaultPartitioner((string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                            {
                                Assert.Equal(topic.Name, topicName);
                                var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                                return int.Parse(keyString.Split(" ").Last()) % partitionCount;
                            });
                            break;
                        case 2:
                            // Default custom partitioner in case where default topic config is present due to topic level config in top-level config.
                            var producerConfig2 = new ProducerConfig
                            {
                                BootstrapServers = bootstrapServers,
                                MessageTimeoutMs = 10000
                            };
                            producerBuilder = new ProducerBuilder<string, string>(producerConfig2);
                            producerBuilder.SetDefaultPartitioner((string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                            {
                                Assert.Equal(topic.Name, topicName);
                                var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                                return int.Parse(keyString.Split(" ").Last()) % partitionCount;
                            });
                            break;
                        default:
                            Assert.True(false);
                            break;
                    }

                    using (var producer = producerBuilder.Build())
                    {
                        for (int i=0; i<PARTITION_COUNT; ++i)
                        {
                            producer.Produce(
                                topic.Name,
                                new Message<string, string> { Key = $"test key {i}", Value = $"test val {i}" }, dh);
                        }

                        producer.Flush(TimeSpan.FromSeconds(10));
                    }
                }
            }


            // Null key
            var partitionerCalledTimes = 0;
            producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                StickyPartitioningLingerMs = 0 // disable sticky partitioning and run the default partitioner for null keys.
            };

            using (var topic = new TemporaryTopic(bootstrapServers, PARTITION_COUNT))
            using (var producer = new ProducerBuilder<Null, string>(producerConfig)
                .SetDefaultPartitioner((string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                {
                    Assert.True(keyIsNull);
                    partitionerCalledTimes++;
                    return 0;
                })
                .Build())
            {
                producer.Produce(topic.Name, new Message<Null, string> { Value = "test value" });
                producer.Flush(TimeSpan.FromSeconds(10));
                Assert.Equal(1, partitionerCalledTimes);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_CustomPartitioner");
        }

    }
}
