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
        ///     Basic DeserializingConsumer test (consume mode).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Subscription(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Subscription");
            
            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 1, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            using (var consumer = new ConsumerBuilder(consumerConfig).Build())
            {
                // Test empty case.
                Assert.Empty(consumer.Subscription);

                consumer.SetPartitionsAssignedHandler((_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(firstProduced.TopicPartition, partitions[0]);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                    // test non-empty case.
                    Assert.Single(consumer.Subscription);
                    Assert.Equal(singlePartitionTopic, consumer.Subscription[0]);
                });

                consumer.SetPartitionsRevokedHandler((_, partitions)
                    => consumer.Unassign());

                consumer.Subscribe(singlePartitionTopic);

                var r = consumer.Consume(TimeSpan.FromSeconds(10));

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Subscription");
        }

    }
}
