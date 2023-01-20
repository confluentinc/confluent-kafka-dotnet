// Copyright 2022 Confluent Inc.
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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test regex subscription in scenario where new topics are added.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Subscription_Regex(string bootstrapServers)
        {
            LogToFile("start Consumer_Subscription_Regex");

            var topicMetadataRefreshPeriodMs = 1000;
            var rebalanceWaitMs = 2000;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                TopicMetadataRefreshIntervalMs = topicMetadataRefreshPeriodMs
            };

            string prefix = "dotnet_test_" + Guid.NewGuid().ToString() + "_";

            using (var topic1 = new TemporaryTopic(prefix, bootstrapServers, 1))
            using (var topic2 = new TemporaryTopic(prefix, bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic1.Name, 100, 100);
                Util.ProduceNullStringMessages(bootstrapServers, topic2.Name, 100, 100);

                consumer.Subscribe("^" + prefix + ".*$");

                // Wait for assignment
                consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(2, consumer.Assignment.Count);

                // Create new matching topic, wait long enough for metadata to be
                // discovered and corresponding rebalance.
                using var topic3 = new TemporaryTopic(prefix, bootstrapServers, 1);
                Thread.Sleep(topicMetadataRefreshPeriodMs + rebalanceWaitMs);
                consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(3, consumer.Assignment.Count);

                // Repeat a couple more times... 
                using var topic4 = new TemporaryTopic(prefix, bootstrapServers, 1);
                Thread.Sleep(topicMetadataRefreshPeriodMs + rebalanceWaitMs);
                consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer.Assignment.Count);

                using var topic5 = new TemporaryTopic(prefix, bootstrapServers, 1);
                Thread.Sleep(topicMetadataRefreshPeriodMs + rebalanceWaitMs);
                consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(5, consumer.Assignment.Count);

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Subscription_Regex");
        }

    }
}
