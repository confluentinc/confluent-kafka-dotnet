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

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     This is an experiment to see what happens when two consumers in the
        ///     same group read from the same topic/partition.
        /// </summary>
        /// <remarks>
        ///     You should never do this, but the brokers don't actually prevent it.
        /// </remarks>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void DuplicateConsumerAssign(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "duplicate-consumer-assign-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                Assert.NotNull(dr);
                producer.Flush();
            }

            using (var consumer1 = new Consumer(consumerConfig))
            using (var consumer2 = new Consumer(consumerConfig))
            {
                consumer1.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, 0) });
                consumer2.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, dr.Partition, 0) });
                Message msg;
                var haveMsg1 = consumer1.Consume(out msg, TimeSpan.FromSeconds(10));
                Assert.NotNull(msg);
                var haveMsg2 = consumer2.Consume(out msg, TimeSpan.FromSeconds(10));
                Assert.NotNull(msg);

                // NOTE: two consumers from the same group should never be assigned to the same
                // topic / partition. This 'test' is here because I was curious to see what happened
                // in practice if this did occur. Because this is not expected usage, no validation
                // has been included in this test.
            }
        }

    }
}
