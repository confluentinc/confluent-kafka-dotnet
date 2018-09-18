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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic test of Consumer.Seek.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_Seek(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_Seek");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers
            };

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            using (var producer = new Producer<Null, string>(producerConfig))
            using (var consumer = new Consumer<Null, string>(consumerConfig))
            {
                consumer.OnError += (_, e)
                    => Assert.True(false, e.Reason);

                const string checkValue = "check value";
                var dr = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = checkValue }).Result;
                var dr2 = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "second value" }).Result;
                var dr3 = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "third value" }).Result;

                consumer.Assign(new TopicPartitionOffset[] { new TopicPartitionOffset(singlePartitionTopic, 0, dr.Offset) });

                ConsumeResult<Null, string> record = consumer.Consume(TimeSpan.FromSeconds(30));
                Assert.NotNull(record.Message);
                record = consumer.Consume(TimeSpan.FromSeconds(30));
                Assert.NotNull(record.Message);
                record = consumer.Consume(TimeSpan.FromSeconds(30));
                Assert.NotNull(record.Message);
                consumer.Seek(dr.TopicPartitionOffset);

                record = consumer.Consume(TimeSpan.FromSeconds(30));
                Assert.NotNull(record.Message);
                Assert.Equal(checkValue, record.Message.Value);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_Seek");
        }

    }
}
