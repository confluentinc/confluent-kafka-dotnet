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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Basic test of OffsetsCommittedHandler.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_OffsetsCommittedHandler(string bootstrapServers)
        {
            LogToFile("start Consumer_OffsetsCommittedHandler");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 2000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            var committedCount = 0;
            
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetOffsetsCommittedHandler((_, o) =>
                    {
                        Assert.Equal(ErrorCode.NoError, o.Error.Code);
                        Assert.Single(o.Offsets);
                        Assert.Equal(0, o.Offsets[0].Partition.Value);
                        Assert.Equal(singlePartitionTopic, o.Offsets[0].Topic);
                        Assert.Equal(new TopicPartition(singlePartitionTopic, 0), o.Offsets[0].TopicPartition);
                        Assert.Equal(new TopicPartition(singlePartitionTopic, 0), o.Offsets[0].TopicPartitionOffset.TopicPartition);
                        Assert.True(o.Offsets[0].Offset >= 0);
                        committedCount += 1;
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                var startTime = DateTime.MinValue;
                while (startTime == DateTime.MinValue || DateTime.Now - startTime < TimeSpan.FromSeconds(6))
                {
                    var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr == null) { continue; }
                    if (startTime == DateTime.MinValue) { startTime = DateTime.Now; }
                }

                Assert.True(committedCount > 0);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_OffsetsCommittedHandler");
        }

    }
}
