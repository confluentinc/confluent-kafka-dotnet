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
        ///     Test various combinations of unsubscribing / committing before disposing the consumer.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Exiting(string bootstrapServers)
        {
            LogToFile("start Consumer_Exiting");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                Debug = "all"
            };

            for (int i=0; i<4; ++i)
            {
                consumerConfig.Set("group.id", Guid.NewGuid().ToString());

                using (var consumer =
                    new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                        .SetPartitionsAssignedHandler((c, partitions) =>
                        {
                            return partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset));
                        })
                        .Build())
                {
                    consumer.Subscribe(singlePartitionTopic);

                    int tryCount = 10;
                    while (tryCount-- > 0)
                    {
                        var record = consumer.Consume(TimeSpan.FromSeconds(10));
                        if (record != null)
                        {
                            break;
                        }
                    }

                    Assert.True(tryCount > 0);

                    // there should be no ill effect doing any of this before disposing a consumer.
                    switch (i)
                    {
                        case 0:
                            LogToFile("  -- Unsubscribe");
                            consumer.Unsubscribe();
                            break;
                        case 1:
                            LogToFile("  -- Commit");
                            consumer.Commit();
                            break;
                        case 3:
                            LogToFile("  -- Close");
                            consumer.Close();
                            break;
                        case 4:
                            break;
                    }
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Exiting");
        }

    }
}
