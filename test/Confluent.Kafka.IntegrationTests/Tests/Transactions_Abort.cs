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
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Tests related to aborted transactions.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_Abort(string bootstrapServers)
        {
            LogToFile("start Transactions_Abort");

            var defaultTimeout = TimeSpan.FromSeconds(30);

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString() }).Build())
            {
                producer.InitTransactions(defaultTimeout);
                producer.BeginTransaction();
                producer.Produce(topic.Name, new Message<string, string> { Key = "test key 0", Value = "test val 0" }, (dr) => {
                    Assert.Equal(0, dr.Offset);
                });
                Thread.Sleep(4000); // ensure the abort ctrl message makes it into the log.
                producer.AbortTransaction(defaultTimeout);
                producer.BeginTransaction();
                producer.Produce(topic.Name, new Message<string, string> { Key = "test key 1", Value = "test val 1" }, (dr) => {
                    // abort marker will be at offset 1.
                    Assert.Equal(2, dr.Offset);
                });
                producer.CommitTransaction(defaultTimeout);

                using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { IsolationLevel = IsolationLevel.ReadCommitted, BootstrapServers = bootstrapServers, GroupId = "unimportant", EnableAutoCommit = false, Debug="all" }).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

                    var cr1 = consumer.Consume();
                    var cr2 = consumer.Consume(TimeSpan.FromMilliseconds(100)); // force the consumer to read over the final control message internally.
                    Assert.Equal("test val 1", cr1.Message.Value);
                    Assert.Equal(2, cr1.Offset); // there should be skipped offsets due to the aborted txn and commit marker in the log.
                    Assert.Null(cr2); // control message should not be exposed to application.
                }

                using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { IsolationLevel = IsolationLevel.ReadUncommitted, BootstrapServers = bootstrapServers, GroupId = "unimportant", EnableAutoCommit = false, Debug="all" }).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

                    var cr1 = consumer.Consume();
                    var cr2 = consumer.Consume();
                    var cr3 = consumer.Consume(TimeSpan.FromMilliseconds(100)); // force the consumer to read over the final control message internally.
                    Assert.Equal("test val 0", cr1.Message.Value);
                    Assert.Equal(0, cr1.Offset); // the aborted message should not be skipped.
                    Assert.Equal("test val 1", cr2.Message.Value);
                    Assert.Equal(2, cr2.Offset); // there should be a skipped offset due to a commit marker in the log.
                    Assert.Null(cr3); // control message should not be exposed to application.
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_Abort");
        }
    }
}
