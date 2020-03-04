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
    /// <summary>
    ///     Simple test case for SendOffsetsToTransaction.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_SendOffsets(string bootstrapServers)
        {
            LogToFile("start Transactions_SendOffsets");

            var defaultTimeout = TimeSpan.FromSeconds(30);

            var groupName = Guid.NewGuid().ToString();
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString() }).Build())
            using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig { IsolationLevel = IsolationLevel.ReadCommitted, BootstrapServers = bootstrapServers, GroupId = groupName, EnableAutoCommit = false, Debug="all" }).Build())
            {
                producer.InitTransactions(defaultTimeout);
                producer.BeginTransaction();
                producer.Produce(topic.Name, new Message<string, string> { Key = "test key 0", Value = "test val 0" });
                producer.SendOffsetsToTransaction(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic.Name, 0, 7324) }, consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(30));
                producer.CommitTransaction(defaultTimeout);
                var committed = consumer.Committed(new List<TopicPartition> { new TopicPartition(topic.Name, 0) }, TimeSpan.FromSeconds(30));
                Assert.Single(committed);
                Assert.Equal(7324, committed[0].Offset);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_SendOffsets");
        }
    }
}
