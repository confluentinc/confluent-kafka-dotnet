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

using System;
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Tests related to watermark offset request behavior and transactions.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_WatermarkOffsets(string bootstrapServers)
        {
            LogToFile("start Transactions_WatermarkOffsets");

            var groupName = Guid.NewGuid().ToString();
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString(), LingerMs = 0 }).Build())
            using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { IsolationLevel = IsolationLevel.ReadCommitted, BootstrapServers = bootstrapServers, GroupId = groupName, EnableAutoCommit = false }).Build())
            {
                var wo1 = consumer.GetWatermarkOffsets(new TopicPartition(topic.Name, 0));
                Assert.Equal(Offset.Unset, wo1.Low);
                Assert.Equal(Offset.Unset, wo1.High);

                consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

                producer.InitTransactions(TimeSpan.FromSeconds(30));
                producer.BeginTransaction();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message1" }).Wait();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message2" }).Wait();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message3" }).Wait();

                WatermarkOffsets wo2 = new WatermarkOffsets(Offset.Unset, Offset.Unset);
                for (int i=0; i<10; ++i)
                {
                    var cr = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    wo2 = consumer.GetWatermarkOffsets(new TopicPartition(topic.Name, 0));
                    if (wo2.High == 3) { break; }
                }
                Assert.Equal(3, wo2.High);
                producer.CommitTransaction(TimeSpan.FromSeconds(30));

                WatermarkOffsets wo3 = new WatermarkOffsets(Offset.Unset, Offset.Unset);
                for (int i=0; i<10; ++i)
                {
                    var cr2 = consumer.Consume(TimeSpan.FromSeconds(500));
                    wo3 = consumer.GetWatermarkOffsets(new TopicPartition(topic.Name, 0));
                    if (wo3.High > 3) { break; }
                }
                Assert.Equal(4, wo3.High);

                var wo4 = consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, 0), TimeSpan.FromSeconds(30));
                Assert.Equal(4, wo4.High);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_WatermarkOffsets");
        }
    }
}
