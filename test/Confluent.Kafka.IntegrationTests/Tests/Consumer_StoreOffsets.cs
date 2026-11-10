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
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test StoreOffsets with multiple offsets at once.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_StoreOffsets(string bootstrapServers)
        {
            LogToFile("start Consumer_StoreOffsets");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };

            using (var topic = new TemporaryTopic(bootstrapServers, 2))
            using (var consumer = new TestConsumerBuilder<Null, string>(consumerConfig).Build())
            {
                Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 100, 1000);

                consumer.Subscribe(topic.Name);

                // wait until consumer is assigned to both partitions.
                ConsumeResult<Null, string> cr = consumer.Consume();
                Assert.Equal(2, consumer.Assignment.Count);

                // store offsets on both partitions at once should not throw.
                consumer.StoreOffsets(new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset(topic.Name, 0, 1),
                    new TopicPartitionOffset(topic.Name, 1, 1)
                });

                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_StoreOffsets");
        }

    }
}
