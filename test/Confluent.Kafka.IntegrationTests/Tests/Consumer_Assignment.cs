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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests of <see cref="Consumer.Assignment" />
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Assignment(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Assignment");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 1, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            // Test in which both receive and revoke events are specified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetRebalanceHandler((c, e) =>
                    {
                        if (e.IsAssignment)
                        {
                            Assert.Single(e.Partitions);
                            Assert.Equal(firstProduced.TopicPartition, e.Partitions[0]);
                            c.Assign(e.Partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                            // test non-empty case.
                            Assert.Single(c.Assignment);
                            Assert.Equal(singlePartitionTopic, c.Assignment[0].Topic);
                            Assert.Equal(0, (int)c.Assignment[0].Partition);
                        }
                        else
                        {
                            Assert.Single(c.Assignment);
                            c.Unassign();
                            Assert.Empty(c.Assignment);
                        }
                    })
                    .Build())
            {
                Assert.Empty(consumer.Assignment);
                consumer.Subscribe(singlePartitionTopic);
                var r = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Close();
            }

            // test in which only the revoked event handler is specified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetRebalanceHandler((c, e) =>
                    {
                        if (e.IsRevocation)
                        {
                            Assert.Single(c.Assignment);
                            c.Unassign();
                            Assert.Empty(c.Assignment);
                        }
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Single(consumer.Assignment);

                consumer.Unsubscribe();

                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(10));

                Assert.Empty(consumer.Assignment);

                consumer.Close();
            }

            // test in which only the receive event handler is specified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetRebalanceHandler((c, e) =>
                    {
                        if (e.IsAssignment)
                        {
                            Assert.Empty(c.Assignment);
                            c.Assign(e.Partitions);
                            Assert.Single(c.Assignment);
                        }
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Single(consumer.Assignment);

                consumer.Unsubscribe();

                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(10));

                Assert.Empty(consumer.Assignment);

                consumer.Close();
            }

            // test in which neither the receive or revoke handler is specified.
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Subscribe(singlePartitionTopic);

                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Single(consumer.Assignment);

                consumer.Unsubscribe();

                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(10));

                Assert.Empty(consumer.Assignment);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Assignment");
        }

    }
}
