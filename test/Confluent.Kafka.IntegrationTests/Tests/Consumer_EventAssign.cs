// Copyright 2019 Confluent Inc.
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
        ///     Test the RebalanceEvent parameter variant of the Assign method.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_EventAssign(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_EventAssign");

            int N = 2;
            var firstProduced = Util.ProduceNullStringMessages(bootstrapServers, singlePartitionTopic, 100, N);

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnablePartitionEof = true
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                int rebalanceCalledCount = 0;
                consumer.SetRebalanceHandler((_, e) =>
                {
                    Assert.Single(e.Partitions);
                    consumer.Assign(e);
                    rebalanceCalledCount += 1;
                });

                consumer.Subscribe(singlePartitionTopic);

                DateTime startTime = DateTime.Now;
                while (rebalanceCalledCount < 1 && DateTime.Now - startTime < TimeSpan.FromSeconds(20))
                    consumer.Consume(TimeSpan.FromMilliseconds(100));
                Assert.Equal(1, rebalanceCalledCount);

                consumer.Unsubscribe();

                startTime = DateTime.Now;
                while (rebalanceCalledCount < 2 && DateTime.Now - startTime < TimeSpan.FromSeconds(20))
                    consumer.Consume(TimeSpan.FromMilliseconds(100));
                Assert.Equal(2, rebalanceCalledCount);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_EventAssign");
        }
    }
}
