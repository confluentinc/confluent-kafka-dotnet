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
        ///     Simple test of both Consumer.Assign overloads.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AssignOverloads(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "assign-overloads-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";
            var testString2 = "hello world 2";

            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                Assert.False(dr.Error.HasError);
                var dr2 = producer.ProduceAsync(topic, null, testString2).Result;
                Assert.False(dr2.Error.HasError);
                producer.Flush();
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                // Explicitly specify partition offset.
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset) });
                Message<Null, string> msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(msg.Value, testString);

                // Determine offset to consume from automatically.
                consumer.Assign(new List<TopicPartition>() { dr.TopicPartition });
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.Equal(msg.Value, testString2);
            }
        }

    }
}
