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
        ///     PartitionEOF related tests.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_PartitionEOF(string bootstrapServers)
        {
            LogToFile("start Consumer_PartitionEOF");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnablePartitionEof = false
            };

            // no eof, non generic consumer case.
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

                var cr1 = consumer.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = consumer.Consume();
                Assert.NotNull(cr2.Message);
                Assert.False(cr2.IsPartitionEOF);
                var cr3 = consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr3);

                consumer.Close();
            }

            // no eof, generic consumer case.
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

                var cr1 = consumer.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = consumer.Consume();
                Assert.NotNull(cr2.Message);
                Assert.False(cr2.IsPartitionEOF);
                var cr3 = consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr3);

                consumer.Close();
            }

            consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnablePartitionEof = true
            };

            // eof, non-generic consumer case.
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

                var cr1 = consumer.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = consumer.Consume();
                Assert.NotNull(cr2.Message);
                Assert.False(cr2.IsPartitionEOF);
                var cr3 = consumer.Consume();
                Assert.Null(cr3.Message);
                Assert.True(cr3.IsPartitionEOF);
                var cr4 = consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr4);

                consumer.Close();
            }

            // eof, generic consumer case.
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

                var cr1 = consumer.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = consumer.Consume();
                Assert.NotNull(cr2.Message);
                Assert.False(cr2.IsPartitionEOF);
                var cr3 = consumer.Consume();
                Assert.Null(cr3.Message);
                Assert.True(cr3.IsPartitionEOF);
                var cr4 = consumer.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr4);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_PartitionEOF");
        }
    }
}
