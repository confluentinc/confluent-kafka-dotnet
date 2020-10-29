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
                BootstrapServers = bootstrapServers
            };

            Action<DeliveryReport<string, string>> dh = (DeliveryReport<string, string> dr) =>
            {
                Assert.StartsWith($"test key ", dr.Message.Key);
                Assert.StartsWith($"test val ", dr.Message.Value);
                var expectedPartition = int.Parse(dr.Message.Key.Split(" ").Last());
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(PersistenceStatus.Persisted, dr.Status);
                Assert.Equal(expectedPartition, (int)dr.Partition);
                Assert.True(dr.Offset >= 0);
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
            };

            int createCount = 0;
            using (var topic = new TemporaryTopic(bootstrapServers, PARTITION_COUNT))
            using (var producer =
                new ProducerBuilder<string, string>(producerConfig)
                    .SetPartitioner(topic.Name, (string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) => {
                        return createCount++ % partitionCount;
                    })
                .Build())
            {
                for (int i=0; i<PARTITION_COUNT; ++i)
                {
                    producer.Produce(
                        topic.Name,
                        new Message<string, string> { Key = $"test key {i}", Value = $"test val {i}" }, dh);
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_CustomPartitioner");
        }
    }
}
