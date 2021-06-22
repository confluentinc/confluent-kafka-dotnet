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


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     This is an experiment to see what happens when two consumers in the
        ///     same group read from the same topic/partition.
        /// </summary>
        /// <remarks>
        ///     You should never do this, but the brokers don't actually prevent it.
        /// </remarks>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void DuplicateConsumerAssign(string bootstrapServers)
        {
            LogToFile("start DuplicateConsumerAssign");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var testString = "hello world";

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                DeliveryResult<byte[], byte[]> dr;
                using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
                {
                    dr = producer.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize(testString, SerializationContext.Empty) }).Result;
                    Assert.NotNull(dr);
                    producer.Flush(TimeSpan.FromSeconds(10));
                }

                using (var consumer1 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                using (var consumer2 = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer1.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic.Name, dr.Partition, 0) });
                    consumer2.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic.Name, dr.Partition, 0) });
                    ConsumeResult<byte[], byte[]> record;
                    record = consumer1.Consume(TimeSpan.FromSeconds(10));
                    Assert.NotNull(record);
                    Assert.NotNull(record.Message);
                    record = consumer2.Consume(TimeSpan.FromSeconds(10));
                    Assert.NotNull(record);
                    Assert.NotNull(record.Message);

                    // NOTE: two consumers from the same group should never be assigned to the same
                    // topic / partition. This 'test' is here because I was curious to see what happened
                    // in practice if this did occur. Because this is not expected usage, no validation
                    // has been included in this test.
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   DuplicateConsumerAssign");
        }

    }
}
