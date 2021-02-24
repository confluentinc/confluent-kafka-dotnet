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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests of <see cref="Consumer.Assignment" />
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Assignment(string bootstrapServers)
        {
            LogToFile("start Consumer_Assignment");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 1, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnableAutoCommit = false
            };

            // Test in which both receive and revoke events are specified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Assert.Single(partitions);
                        Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                        Assert.Empty(c.Assignment);
                        return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Assert.Single(c.Assignment);
                    })
                    .Build())
            {
                Assert.Empty(consumer.Assignment);
                consumer.Subscribe(singlePartitionTopic);
                var r = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Close();
            }

            // test in which only the revoked event handler is specified
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Assert.Single(c.Assignment);
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Single(consumer.Assignment);
                consumer.Unsubscribe();
                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Close();
            }

            // test in which only the revoked event handler is specified
            // and the returned set unmodified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Assert.Single(c.Assignment);
                        return partitions;
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Single(consumer.Assignment);
                consumer.Unsubscribe();
                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Close();
            }


            // test in which only the receive event handler is specified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Assert.Empty(c.Assignment);
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Single(consumer.Assignment);
                consumer.Unsubscribe();
                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Close();
            }

            // test in which only the receive event handler is specified
            // and assignment set is modified.
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Assert.Empty(c.Assignment);
                        Assert.Single(partitions);
                        return new List<TopicPartitionOffset>();
                    })
                    .Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Unsubscribe();
                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Close();
            }

            // test in which neither the receive or revoke handler is specified.
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                // assignment will happen as a side effect of this:
                var r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Single(consumer.Assignment);
                consumer.Unsubscribe();
                // revoke will happen as side effect of this:
                r = consumer.Consume(TimeSpan.FromSeconds(4));
                Assert.Empty(consumer.Assignment);
                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Assignment");
        }

    }
}
