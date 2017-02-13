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
        ///     Test that a specific user specified timestamp on produce is consumed.
        /// </summary>
        [Theory (Skip="currently no rd_kafka_producev in referenced librdkafka"), MemberData(nameof(KafkaParameters))]
        public static void ProduceTimestamp(string bootstrapServers, string topic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 },
                { "api.version.request", true }
            };

            var testString = "hello world";
            var testTime = new DateTime(2010, 1, 1, 0, 0, 0);

            var s = Library.VersionString;
            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString, testTime).Result;
                Assert.NotNull(dr);
                Assert.Equal(topic, dr.Topic);
                Assert.NotEqual<long>(dr.Offset, Offset.Invalid);
                Assert.Equal(dr.Timestamp.DateTime, testTime);
                Assert.True(false); // TODO: check timestamp type. what should it be?
                producer.Flush();
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });
                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));
                Assert.NotNull(msg);
                Assert.Equal(msg.Timestamp.DateTime, testTime);
                Assert.True(false); // TODO: check timestamp type. what should it be?
            }
        }

    }
}
