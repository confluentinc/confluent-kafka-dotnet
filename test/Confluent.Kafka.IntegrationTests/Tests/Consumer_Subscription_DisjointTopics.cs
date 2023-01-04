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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        private void DisjointTopicsSubscribeTest(String bootstrapServers, PartitionAssignmentStrategy assignmentStrategy)
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                PartitionAssignmentStrategy = assignmentStrategy
            };

            using (var topic1 = new TemporaryTopic(bootstrapServers, 4))
            using (var topic2 = new TemporaryTopic(bootstrapServers, 4))
            using (var topic3 = new TemporaryTopic(bootstrapServers, 4))
            using (var topic4 = new TemporaryTopic(bootstrapServers, 4))
            using (var consumer1 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var consumer2 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var consumer3 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var consumer4 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var consumer5 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            using (var consumer6 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic1.Name, 100, 1000);
                Util.ProduceNullStringMessages(bootstrapServers, topic2.Name, 100, 1000);
                Util.ProduceNullStringMessages(bootstrapServers, topic3.Name, 100, 1000);
                Util.ProduceNullStringMessages(bootstrapServers, topic4.Name, 100, 1000);

                consumer1.Subscribe(topic1.Name);
                // Consume -> wait for assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer1.Assignment.Count);
                Assert.Equal(topic1.Name, consumer1.Assignment[0].Topic);

                consumer2.Subscribe(topic2.Name);
                // Allow rebalance to complete 
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer1.Assignment.Count);
                Assert.Equal(topic1.Name, consumer1.Assignment[0].Topic);
                consumer2.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer2.Assignment.Count);
                Assert.Equal(topic2.Name, consumer2.Assignment[0].Topic);

                consumer3.Subscribe(topic3.Name);
                // Allow rebalance to complete 
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer1.Assignment.Count);
                Assert.Equal(topic1.Name, consumer1.Assignment[0].Topic);
                consumer2.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer2.Assignment.Count);
                Assert.Equal(topic2.Name, consumer2.Assignment[0].Topic);
                consumer3.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer3.Assignment.Count);
                Assert.Equal(topic3.Name, consumer3.Assignment[0].Topic);

                consumer4.Subscribe(topic4.Name);
                // Allow rebalance to complete 
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer1.Assignment.Count);
                Assert.Equal(topic1.Name, consumer1.Assignment[0].Topic);
                consumer2.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer2.Assignment.Count);
                Assert.Equal(topic2.Name, consumer2.Assignment[0].Topic);
                consumer3.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer3.Assignment.Count);
                Assert.Equal(topic3.Name, consumer3.Assignment[0].Topic);
                consumer4.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(4, consumer4.Assignment.Count);
                Assert.Equal(topic4.Name, consumer4.Assignment[0].Topic);

                consumer5.Subscribe(topic2.Name);
                // Allow rebalance to complete 
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(2, consumer2.Assignment.Count);
                Assert.Equal(2, consumer5.Assignment.Count);
                Assert.Equal(topic2.Name, consumer2.Assignment[0].Topic);
                Assert.Equal(topic2.Name, consumer5.Assignment[0].Topic);

                consumer6.Subscribe(new List<string> { topic3.Name, topic4.Name });
                // Allow rebalance to complete 
                consumer6.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));
                Assert.True(consumer3.Assignment.Count > 0);
                Assert.True(consumer4.Assignment.Count > 0);
                Assert.True(consumer6.Assignment.Count > 0);
                Assert.Equal(8, consumer3.Assignment.Count + consumer4.Assignment.Count + consumer6.Assignment.Count);
                Assert.Equal(topic3.Name, consumer3.Assignment[0].Topic);
                Assert.Equal(topic4.Name, consumer4.Assignment[0].Topic);
                Assert.True(consumer6.Assignment[0].Topic == topic3.Name || consumer6.Assignment[0].Topic == topic4.Name);

                consumer1.Unsubscribe();
                // Allow rebalance to complete 
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));

                Assert.Equal(0, consumer1.Assignment.Count);
                // Allow rebalance to complete 
                consumer1.Subscribe(topic1.Name);
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));
                // Get the assignment
                consumer1.Consume(TimeSpan.FromSeconds(10));
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer3.Consume(TimeSpan.FromSeconds(10));
                consumer4.Consume(TimeSpan.FromSeconds(10));
                consumer5.Consume(TimeSpan.FromSeconds(10));
                consumer6.Consume(TimeSpan.FromSeconds(10));

                Assert.Equal(4, consumer1.Assignment.Count);
                Assert.Equal(topic1.Name, consumer1.Assignment[0].Topic);

                consumer1.Close();
                consumer2.Close();
                consumer3.Close();
                consumer4.Close();
                consumer5.Close();
                consumer6.Close();
            }
        }

        /// <summary>
        ///     Check various scenarios where the same consumer group subscribes to
        ///     different topics in a disjoint fashion.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Subscription_DisjointTopics(string bootstrapServers)
        {
            LogToFile("start Consumer_Subscription_DisjointTopics");

            DisjointTopicsSubscribeTest(bootstrapServers, PartitionAssignmentStrategy.Range);
            DisjointTopicsSubscribeTest(bootstrapServers, PartitionAssignmentStrategy.RoundRobin);
            DisjointTopicsSubscribeTest(bootstrapServers, PartitionAssignmentStrategy.CooperativeSticky);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Subscription_DisjointTopics");
        }

    }
}
