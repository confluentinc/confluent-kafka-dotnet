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
using System.Linq;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Basic Consume test.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Consume(string bootstrapServers)
        {
            LogToFile("start Consumer_Consume");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true
            };

            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Assert.Single(partitions);
                        Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                        return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                int msgCnt = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }

                    Assert.Equal(TimestampType.CreateTime, record.Message.Timestamp.Type);
                    Assert.True(Math.Abs((DateTime.UtcNow - record.Message.Timestamp.UtcDateTime).TotalMinutes) < 10.0);
                    msgCnt += 1;
                }

                Assert.Equal(msgCnt, N);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Consume");
        }

    }
}
