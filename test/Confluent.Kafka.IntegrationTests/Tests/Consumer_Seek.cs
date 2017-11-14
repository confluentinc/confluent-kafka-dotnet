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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
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
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "acks", "all" },
                { "bootstrap.servers", bootstrapServers }
            };

            var producerConfig = new Dictionary<string, object> { {"bootstrap.servers", bootstrapServers}};

            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnError += (_, e) =>
                    Assert.False(true);

                consumer.OnConsumeError += (_, e) =>
                    Assert.False(true);

                const string checkValue = "check value";
                var dr = producer.ProduceAsync(singlePartitionTopic, null, checkValue).Result;
                var dr2 = producer.ProduceAsync(singlePartitionTopic, null, "second value").Result;
                var dr3 = producer.ProduceAsync(singlePartitionTopic, null, "third value").Result;

                consumer.Assign(new TopicPartitionOffset[] { new TopicPartitionOffset(singlePartitionTopic, 0, dr.Offset) });

                Message<Null, string> message;
                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(30)));
                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(30)));
                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(30)));
                consumer.Seek(dr.TopicPartitionOffset);

                Assert.True(consumer.Consume(out message, TimeSpan.FromSeconds(30)));
                Assert.Equal(checkValue, message.Value);
            }
        }

    }
}
