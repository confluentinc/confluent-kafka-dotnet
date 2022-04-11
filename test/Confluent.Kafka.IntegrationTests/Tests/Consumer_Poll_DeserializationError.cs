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
using System.Text;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test exceptions are thrown appropriately during deserialization of keys
        ///     and values.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Poll_DeserializationError(string bootstrapServers)
        {
            LogToFile("start Consumer_Poll_DeserializationError");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            TopicPartitionOffset firstProduced = null;
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                var keyData = Encoding.UTF8.GetBytes("key");
                firstProduced = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Key = keyData }).Result.TopicPartitionOffset;
                var valData = Encoding.UTF8.GetBytes("val");
                producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Value = valData });
                Assert.True(producer.Flush(TimeSpan.FromSeconds(10)) == 0);
            }

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true
            };

            // test key deserialization error behavior
            using (var consumer =
                new ConsumerBuilder<Null, string>(consumerConfig)
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
                int errCnt = 0;
                while (true)
                {
                    var s = consumer.Subscription;
                    try
                    {
                        var record = consumer.Consume(TimeSpan.FromSeconds(10));
                        if (record == null) { continue; }
                        if (record.IsPartitionEOF) { break; }

                        msgCnt += 1;
                    }
                    catch (ConsumeException e)
                    {
                        errCnt += 1;
                        Assert.Equal(ErrorCode.Local_KeyDeserialization, e.Error.Code);
                        Assert.Equal(firstProduced.Offset.Value, e.ConsumerRecord.Offset.Value);
                    }
                }

                Assert.Equal(1, msgCnt);
                Assert.Equal(1, errCnt);

                consumer.Close();
            }

            // test value deserialization error behavior.
            using (var consumer =
                new ConsumerBuilder<string, Null>(consumerConfig)
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
                int errCnt = 0;
                while (true)
                {
                    try
                    {
                        var record = consumer.Consume(TimeSpan.FromSeconds(10));
                        if (record == null) { continue; }
                        if (record.IsPartitionEOF) { break; }

                        msgCnt += 1;
                    }
                    catch (ConsumeException e)
                    {
                        errCnt += 1;
                        Assert.Equal(ErrorCode.Local_ValueDeserialization, e.Error.Code);
                        Assert.Equal(firstProduced.Offset.Value + 1, e.ConsumerRecord.Offset.Value);
                    }
                }

                Assert.Equal(1, msgCnt);
                Assert.Equal(1, errCnt);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Poll_DeserializationError");
        }

    }
}
