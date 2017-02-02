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
                { "group.id", "test-consumer-group" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testString = "hello world";

            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                var dr = producer.ProduceAsync(topic, null, testString).Result;
                Assert.NotEqual((long)dr.Offset, (long)Offset.Invalid); // TODO: remove long cast. this is fixed in another open PR.
                producer.Flush();
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                bool messageHandled = false;
                consumer.OnMessage += (_, message) =>
                {
                    messageHandled = true;
                };

                // Note: handler has not been added to OnPartitionsAssigned

                consumer.Subscribe(topic);

                // whether or not auto.offset.reset comes into play (i.e. whether or not there happen to be any
                // offsets commited), there should be a message to consume.
                consumer.Poll(TimeSpan.FromSeconds(10));
                Assert.True(messageHandled);
            }

        }

    }
}
