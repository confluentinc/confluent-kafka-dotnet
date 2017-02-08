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

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic Consumer test (consume mode).
        /// </summary>
        [Theory, ClassData(typeof(KafkaParameters))]
        public static void Consumer_Consume(string bootstrapServers, string topic)
        {
            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, topic, 100, N);

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "consumer-consume-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer(consumerConfig))
            {
                bool done = false;

                consumer.OnPartitionEOF += (_, partition)
                    => done = true;

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Assert.Equal(partitions.Count, 1);
                    Assert.Equal(partitions[0], firstProduced.TopicPartition);
                    consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));
                };

                consumer.OnPartitionsRevoked += (_, partitions)
                    => consumer.Unassign();

                consumer.Subscribe(topic);

                int msgCnt = 0;
                while (!done)
                {
                    Message msg;
                    if (consumer.Consume(out msg, TimeSpan.FromMilliseconds(100)))
                    {
                        msgCnt += 1;
                    }
                }

                Assert.Equal(msgCnt, N);
            }
        }

    }
}
