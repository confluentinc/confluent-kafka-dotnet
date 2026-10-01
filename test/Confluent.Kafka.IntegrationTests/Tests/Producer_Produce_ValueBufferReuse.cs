// Copyright 2026 Confluent Inc.
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
using System.Linq;
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Produce() asks librdkafka to copy the value, so the caller can reuse the
        ///     value buffer as soon as Produce() returns.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_ValueBufferReuse(string bootstrapServers)
        {
            LogToFile("start Producer_Produce_ValueBufferReuse");

            const int messageCount = 100;
            var buffer = new byte[16];
            using var topic = new TemporaryTopic(bootstrapServers, 1);

            // The long linger means the batch is built after the buffer has been overwritten.
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers, LingerMs = 1000 };
            using (var producer = new TestProducerBuilder<Null, byte[]>(producerConfig).Build())
            {
                for (int i = 0; i < messageCount; i++)
                {
                    Array.Fill(buffer, (byte)i);
                    producer.Produce(topic.Name, new Message<Null, byte[]> { Value = buffer });
                }
                Array.Fill(buffer, (byte)0xFF);
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };
            using (var consumer = new TestConsumerBuilder<Ignore, byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new TopicPartitionOffset(topic.Name, 0, Offset.Beginning));
                for (int i = 0; i < messageCount; i++)
                {
                    var record = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.NotNull(record);
                    Assert.Equal(Enumerable.Repeat((byte)i, buffer.Length).ToArray(), record.Message.Value);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Produce_ValueBufferReuse");
        }
    }
}
