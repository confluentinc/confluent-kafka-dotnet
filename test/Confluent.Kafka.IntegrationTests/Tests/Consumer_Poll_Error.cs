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
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test exceptions thrown during deserialization of keys
        ///     and values are surfaced via the OnConsumeError event.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Poll_Error(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers }
            };

            TopicPartitionOffset firstProduced = null;
            using (var producer = new Producer<byte[], byte[]>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer()))
            {
                var keyData = Encoding.UTF8.GetBytes("key");
                firstProduced = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Key = keyData }).Result.TopicPartitionOffset;
                var valData = Encoding.UTF8.GetBytes("val");
                producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Value = valData });
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            // test key deserialization error behavior
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                int msgCnt = 0;
                int errCnt = 0;
                bool done = false;

                consumer.OnRecord += (_, msg) =>
                {
                    msgCnt += 1;
                };

                consumer.OnConsumeError += (_, msg) =>
                {
                    errCnt += 1;
                    Assert.Equal(ErrorCode.Local_KeyDeserialization, msg.Error.Code);
                    Assert.Equal(firstProduced.Offset.Value, msg.Offset.Value);
                };

                consumer.OnPartitionEOF += (_, partition)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(singlePartitionTopic);

                while (!done)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                Assert.Equal(1, msgCnt);
                Assert.Equal(1, errCnt);
            }

            // test value deserialization error behavior
            using (var consumer = new Consumer<string, Null>(consumerConfig, new StringDeserializer(Encoding.UTF8), null))
            {
                int msgCnt = 0;
                int errCnt = 0;
                bool done = false;

                consumer.OnRecord += (_, record) =>
                {
                    msgCnt += 1;
                };
                
                consumer.OnConsumeError += (_, record) =>
                {
                    errCnt += 1;
                    Assert.Equal(ErrorCode.Local_ValueDeserialization, record.Error.Code);
                    Assert.Equal(firstProduced.Offset.Value + 1, record.Offset.Value);
                };

                consumer.OnPartitionEOF += (_, partition)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(singlePartitionTopic);

                while (!done)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                Assert.Equal(1, msgCnt);
                Assert.Equal(1, errCnt);
            }

        }

    }
}
