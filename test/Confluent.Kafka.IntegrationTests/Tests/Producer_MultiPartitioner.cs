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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Multi partitioner tests.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_MultiPartitioner(string bootstrapServers)
        {
            LogToFile("start Producer_MultiPartitioner");

            const int PARTITION_COUNT = 33;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            using (var topic1 = new TemporaryTopic(bootstrapServers, PARTITION_COUNT))
            using (var topic2 = new TemporaryTopic(bootstrapServers, PARTITION_COUNT))
            using (var topic3 = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, Null>(producerConfig)
                .SetPartitioner(topic1.Name, (string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                {
                    Assert.Equal(topic1.Name, topicName);
                    var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                    Assert.Equal("hello", keyString);
                    return 8;
                })
                .SetDefaultPartitioner((string topicName, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
                {
                    Assert.True(topic2.Name == topicName || topic3.Name == topicName);
                    var keyString = System.Text.UTF8Encoding.UTF8.GetString(keyData.ToArray());
                    Assert.True(keyString == "world" || keyString == "kafka");
                    return 13;
                })
                .Build()
            ) {

                Action<DeliveryReport<string, Null>> dh = (DeliveryReport<string, Null> dr) =>
                {
                    Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                    Assert.Equal(PersistenceStatus.Persisted, dr.Status);
                    Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                    if (dr.Topic == topic1.Name)
                    {
                        Assert.Equal("hello", dr.Message.Key);
                    }
                    else
                    {
                        Assert.Equal("world", dr.Message.Key);
                    }
                };

                producer.Produce(topic1.Name, new Message<string, Null> { Key = "hello" }, dh);
                producer.Produce(topic2.Name, new Message<string, Null> { Key = "world" }, dh);
                // both default and topic-specific partitioners return a fixed value > number of partitions
                // in topic 3. If either of these partitioners is errantly used in producing this message,
                // this test will fail most of the time.
                producer.Produce(topic3.Name, new Message<string, Null> { Key = "kafka" }, dh);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_MultiPartitioner");
        }

    }
}
