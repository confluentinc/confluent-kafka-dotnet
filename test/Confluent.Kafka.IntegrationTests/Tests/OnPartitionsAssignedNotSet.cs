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
    /// <summary>
    ///     Test that Assign gets called automatically in a scenario where a
    ///     handler has not been added to OnPartitionsAssigned
    ///     (deserializing Consumer)
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void OnPartitionsAssignedNotSet(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "on-partitions-assigned-not-set-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            // Producing onto the topic to make sure it exists.
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                var dr = producer.ProduceAsync(topic, null, "test string").Result;
                Assert.NotEqual((long)dr.Offset, (long)Offset.Invalid); // TODO: remove long cast. this is fixed in PR #29
                producer.Flush();
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(topic);
                Assert.Equal(consumer.Assignment.Count, 0);
                consumer.Poll(TimeSpan.FromSeconds(1));
                Assert.Equal(consumer.Assignment.Count, 1);
                Assert.Equal(consumer.Assignment[0].Topic, topic);
            }
        }
    }
}
