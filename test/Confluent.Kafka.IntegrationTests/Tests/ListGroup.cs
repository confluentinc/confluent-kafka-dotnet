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
        ///     Test getting group information for existant and non-existant
        ///     group using the ListGroup method (on a Consumer).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void ListGroup(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var groupId = Guid.NewGuid().ToString();
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", groupId },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                bool done = false;

                consumer.OnRecord += (_, record)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Single(partitions);
                    Assert.Equal(partitions[0], firstProduced.TopicPartition);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.Subscribe(singlePartitionTopic);

                while (!done)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                var g = consumer.ListGroup(groupId);
                Assert.NotNull(g);
                Assert.Equal(ErrorCode.NoError, g.Error.Code);
                Assert.Equal(groupId, g.Group);
                Assert.Equal("consumer", g.ProtocolType);
                Assert.Single(g.Members);

                g = consumer.ListGroup("non-existent-cg");
                Assert.Null(g);
            }
        }

    }
}
