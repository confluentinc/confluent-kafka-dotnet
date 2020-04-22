// Copyright 2020 Confluent Inc.
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
        /// <summary>
        ///     Test <see cref="Consumer.Assign" /> with Offset.Beginning and Offset.End
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_Assign(string bootstrapServers)
        {
            LogToFile("start Consumer_Assign");

            int N = 5;

            Action<IConsumer<byte[], byte[]>> test = consumer => {
                using (var topic = new TemporaryTopic(bootstrapServers, 1))
                {
                    Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 1, N);
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, Offset.Beginning));
                    var cr1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(0, cr1.Offset);
                    // test that if consumer is paused and reassigned, this does not affect offset.
                    consumer.Pause(new List<TopicPartition> { new TopicPartition(topic.Name, 0) });
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, Offset.End));
                    var cr2 = consumer.Consume(1000);
                    Assert.Null(cr2);
                    Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 1, N);
                    consumer.Resume(new List<TopicPartition> { new TopicPartition(topic.Name, 0) });
                    var cr3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(N, cr3.Offset);
                    var p = consumer.Position(new TopicPartition(topic.Name, 0));
                    Assert.Equal(N+1, p.Value);
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, Offset.Beginning));
                    var cr4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(0, cr4.Offset);
                }
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Error
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                test(consumer);
            }

            // committing offsets should have no effect.
            consumerConfig.EnableAutoCommit = true;
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                test(consumer);
            }
    
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Assign");
        }
    }
}
