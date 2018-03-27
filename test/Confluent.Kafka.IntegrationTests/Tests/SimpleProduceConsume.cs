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
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that produces a message then consumes it.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void SimpleProduceConsume(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            string testString1 = "hello world";
            string testString2 = null;

            DeliveryReport<Null, string> produceResult1;
            DeliveryReport<Null, string> produceResult2;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                produceResult1 = ProduceMessage(singlePartitionTopic, producer, testString1);
                produceResult2 = ProduceMessage(singlePartitionTopic, producer, testString2);
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                ConsumeMessage(consumer, produceResult1, testString1);
                ConsumeMessage(consumer, produceResult2, testString2);
            }
        }

        private static void ConsumeMessage(Consumer consumer, DeliveryReport<Null, string> dr, string testString)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {dr.TopicPartitionOffset});
            ConsumerRecord r;
            Assert.True(consumer.Consume(out r, TimeSpan.FromSeconds(10)));
            Assert.NotNull(r);
            Assert.Equal(testString, r.Message.Value == null ? null : Encoding.UTF8.GetString(r.Message.Value, 0, r.Message.Value.Length));
            Assert.Null(r.Message.Key);
            Assert.Equal(r.Message.Timestamp.Type, dr.Message.Timestamp.Type);
            Assert.Equal(r.Message.Timestamp.UnixTimestampMs, dr.Message.Timestamp.UnixTimestampMs);
        }

        private static DeliveryReport<Null, string> ProduceMessage(string topic, Producer<Null, string> producer, string testString)
        {
            var result = producer.ProduceAsync(topic, new Message<Null, string> { Value = testString }).Result;
            Assert.NotNull(result);
            Assert.Equal(topic, result.Topic);
            Assert.NotEqual<long>(result.Offset, Offset.Invalid);
            Assert.Equal(TimestampType.CreateTime, result.Message.Timestamp.Type);
            Assert.True(Math.Abs((DateTime.UtcNow - result.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
            producer.Flush(TimeSpan.FromSeconds(10));
            return result;
        }
    }
}
