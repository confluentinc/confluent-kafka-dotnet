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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test behavior of StoreOffset on unassigned partition.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_StoreOffset_ErrState(string bootstrapServers)
        {
            LogToFile("start Consumer_StoreOffset_ErrState");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 2))
            using (var consumer1 = new ConsumerBuilder<Null, string>(consumerConfig).Build())
            using (var consumer2 = new ConsumerBuilder<Null, string>(consumerConfig).Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 100, 1000);

                consumer1.Subscribe(topic.Name);

                // wait until consumer is assigned to both partitions.
                ConsumeResult<Null, string> cr = consumer1.Consume();
                Assert.Equal(2, consumer1.Assignment.Count);

                // store offsets on both partitions should not throw.
                consumer1.StoreOffset(new TopicPartitionOffset(topic.Name, 0, 1));
                consumer1.StoreOffset(new TopicPartitionOffset(topic.Name, 1, 1));

                consumer2.Subscribe(topic.Name);

                // wait until each consumer is assigned to one partition.
                consumer2.Consume(TimeSpan.FromSeconds(10));
                consumer1.Consume(TimeSpan.FromSeconds(10));
                
                cr = consumer2.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal(1, consumer1.Assignment.Count);

                // StoreOffset should throw when attempting to assign to a
                // partition no longer assigned.
                bool threw = false;
                try
                {
                    consumer1.StoreOffset(new TopicPartitionOffset(topic.Name, 0, 2));
                    consumer1.StoreOffset(new TopicPartitionOffset(topic.Name, 1, 2));
                }
                catch (KafkaException e)
                {
                    Assert.Equal(ErrorCode.Local_State, e.Error.Code);
                    Assert.False(e.Error.IsFatal);
                    threw = true;
                }
                Assert.True(threw);

                consumer1.Close();
                consumer2.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_StoreOffset_ErrState");
        }

    }
}
