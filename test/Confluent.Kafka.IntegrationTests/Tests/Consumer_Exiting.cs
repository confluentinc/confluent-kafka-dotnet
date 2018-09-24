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
        ///     Test various combinations of unsubscribing / commiting before disposing the consumer.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Exiting(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Consume");

            int N = 2;
            var firstProduced = Util.ProduceMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            for (int i=0; i<4; ++i)
            {
                using (var consumer = new Consumer<Null, string>(consumerConfig))
                {
                    consumer.OnPartitionsAssigned += (_, partitions)
                        => consumer.Assign(partitions.Select(p => new TopicPartitionOffset(p, firstProduced.Offset)));

                    consumer.OnPartitionsRevoked += (_, partitions)
                        => consumer.Unassign();

                    consumer.Subscribe(singlePartitionTopic);

                    int tryCount = 10;
                    while (tryCount-- > 0)
                    {
                        ConsumeResult<Null, string> record = consumer.Consume(TimeSpan.FromMilliseconds(1000));
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
                            consumer.Unsubscribe();
                            break;
                        case 1:
                            consumer.Commit();
                            break;
                        case 3:
                            consumer.Close();
                            break;
                        case 4:
                            break;
                    }
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Consume");
        }

    }
}
