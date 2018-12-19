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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     PartitionEOF related tests.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_PartitionEOF(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
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
            using (var c = new Consumer(consumerConfig))
            {
                c.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    c.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };
                c.Subscribe(singlePartitionTopic);

                var cr1 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr3 = c.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr3);

                c.Close();
            }

            // no eof, generic consumer case.
            using (var c = new Consumer<Null, string>(consumerConfig))
            {
                c.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    c.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };
                c.Subscribe(singlePartitionTopic);

                var cr1 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr3 = c.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr3);

                c.Close();
            }

            consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnablePartitionEof = true
            };

            // eof, non-generic consumer case.
            using (var c = new Consumer(consumerConfig))
            {
                c.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    c.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };
                c.Subscribe(singlePartitionTopic);

                var cr1 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr3 = c.Consume();
                Assert.Null(cr3.Message);
                Assert.True(cr3.IsPartitionEOF);
                var cr4 = c.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr4);

                c.Close();
            }

            // eof, generic consumer case.
            using (var c = new Consumer<Null, string>(consumerConfig))
            {
                c.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    c.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };
                c.Subscribe(singlePartitionTopic);

                var cr1 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr2 = c.Consume();
                Assert.NotNull(cr1.Message);
                Assert.False(cr1.IsPartitionEOF);
                var cr3 = c.Consume();
                Assert.Null(cr3.Message);
                Assert.True(cr3.IsPartitionEOF);
                var cr4 = c.Consume(TimeSpan.FromSeconds(1));
                Assert.Null(cr4);

                c.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_PartitionEOF");
        }
    }
}
