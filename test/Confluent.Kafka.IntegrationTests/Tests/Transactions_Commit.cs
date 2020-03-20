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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test that messages in committed transactions are
    ///     available for consumption as expected, and also
    ///     that the consumer commits offsets as expected when
    ///     there are transactional ctrl messages in the log.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_Commit(string bootstrapServers)
        {
            LogToFile("start Transactions_Commit");

            var defaultTimeout = TimeSpan.FromSeconds(30);

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString() }).Build())
                using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = "unimportant", EnableAutoCommit = false, Debug="all" }).Build())
                {
                    var wm = consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, 0), defaultTimeout);
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, wm.High));

                    producer.InitTransactions(defaultTimeout);
                    producer.BeginTransaction();
                    producer.Produce(topic.Name, new Message<string, string> { Key = "test key 0", Value = "test val 0" });
                    producer.CommitTransaction(defaultTimeout);
                    producer.BeginTransaction();
                    producer.Produce(topic.Name, new Message<string, string> { Key = "test key 1", Value = "test val 1" });
                    producer.CommitTransaction(defaultTimeout);

                    var cr1 = consumer.Consume();
                    var cr2 = consumer.Consume();
                    var cr3 = consumer.Consume(TimeSpan.FromMilliseconds(100)); // force the consumer to read over the final control message internally.
                    Assert.Equal(wm.High, cr1.Offset);
                    Assert.Equal(wm.High+2, cr2.Offset); // there should be a skipped offset due to a commit marker in the log.
                    Assert.Null(cr3); // control message should not be exposed to application.

                    // Test that the committed offset accounts for the final ctrl message.
                    consumer.Commit();
                }

                using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString() }).Build())
                using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = "unimportant", EnableAutoCommit = false, AutoOffsetReset=AutoOffsetReset.Latest }).Build())
                {
                    consumer.Assign(new TopicPartition(topic.Name, 0));

                    // call InitTransactions to prevent a race conidtion between a slow txn commit and a quick offset request.
                    producer.InitTransactions(defaultTimeout);
                    var committed = consumer.Committed(defaultTimeout);
                    var wm = consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, 0), defaultTimeout);
                    Assert.Equal(wm.High, committed[0].Offset);
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_Commit");
        }
    }
}
