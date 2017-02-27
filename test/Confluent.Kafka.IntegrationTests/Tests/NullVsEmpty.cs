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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that null and byte[0] keys and values are produced / consumed
        ///     as expected.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void NullVsEmpty(string bootstrapServers, string topic, string partitionedTopic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers }
            };

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            Message dr;
            using (var producer = new Producer(producerConfig))
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(topic, (byte[])null, null).Result;
                producer.ProduceAsync(topic, null, new byte[0]).Wait();
                producer.ProduceAsync(topic, new byte[0], null).Wait();
                producer.ProduceAsync(topic, new byte[0], new byte[0]).Wait();
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });

                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.NotNull(msg);
                Assert.Null(msg.Key);
                Assert.Equal(msg.Value, new byte[0]);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Equal(msg.Key, new byte[0]);
                Assert.Null(msg.Value);

                Assert.True(consumer.Consume(out msg, TimeSpan.FromMinutes(1)));
                Assert.Equal(msg.Key, new byte[0]);
                Assert.Equal(msg.Value, new byte[0]);
            }
        }

    }
}
