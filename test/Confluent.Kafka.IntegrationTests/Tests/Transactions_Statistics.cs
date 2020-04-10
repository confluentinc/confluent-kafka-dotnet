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
using Newtonsoft.Json.Linq;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Tests related to transactions and the statistics event.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Transactions_Statistics(string bootstrapServers)
        {
            LogToFile("start Transactions_Statistics");

            var groupName = Guid.NewGuid().ToString();

            var cConfig = new ConsumerConfig
            {
                IsolationLevel = IsolationLevel.ReadCommitted,
                BootstrapServers = bootstrapServers,
                GroupId = groupName,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 1000
            };

            var cts = new CancellationTokenSource();

            int ls_offset = -1;
            int hi_offset = -1;
            bool done = false;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers, TransactionalId = Guid.NewGuid().ToString(), LingerMs = 0 }).Build())
            using (var consumer = new ConsumerBuilder<string, string>(cConfig)
                .SetStatisticsHandler((_, json) => {
                    var stats = JObject.Parse(json);
                    ls_offset = (int)stats["topics"][topic.Name]["partitions"]["0"]["ls_offset"];
                    hi_offset = (int)stats["topics"][topic.Name]["partitions"]["0"]["hi_offset"];
                    if (hi_offset > 4) { done = true; }
                })
                .Build())
            {
                consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

                producer.InitTransactions(TimeSpan.FromSeconds(30));
                producer.BeginTransaction();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message_a" }).Wait();
                producer.CommitTransaction(TimeSpan.FromSeconds(30));

                producer.BeginTransaction();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message_b" }).Wait();
                producer.CommitTransaction(TimeSpan.FromSeconds(30));

                producer.BeginTransaction();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message1" }).Wait();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message2" }).Wait();
                producer.ProduceAsync(topic.Name, new Message<string, string> { Key = "test", Value = "message3" }).Wait();

                for (int i=0; i<10; ++i)
                {
                    consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (done) { break; }
                }

                Assert.Equal(4, ls_offset);
                Assert.Equal(7, hi_offset);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Transactions_Statistics");
        }
    }
}
