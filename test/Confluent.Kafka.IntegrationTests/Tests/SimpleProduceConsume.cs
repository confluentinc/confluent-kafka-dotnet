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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     A simple test that produces a couple of messages then
        ///     consumes them back.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void SimpleProduceConsume(string bootstrapServers)
        {
            LogToFile("start SimpleProduceConsume");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            string testString1 = "hello world";
            string testString2 = null;

            DeliveryResult<Null, string> produceResult1;
            DeliveryResult<Null, string> produceResult2;
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                produceResult1 = ProduceMessage(singlePartitionTopic, producer, testString1);
                produceResult2 = ProduceMessage(singlePartitionTopic, producer, testString2);
            }

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                ConsumeMessage(consumer, produceResult1, testString1);
                ConsumeMessage(consumer, produceResult2, testString2);
            }
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   SimpleProduceConsume");
        }

        private static void ConsumeMessage(IConsumer<byte[], byte[]> consumer, DeliveryResult<Null, string> dr, string testString)
        {
            consumer.Assign(new List<TopicPartitionOffset>() {dr.TopicPartitionOffset});
            var r = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.NotNull(r?.Message);
            Assert.Equal(testString, r.Message.Value == null ? null : Encoding.UTF8.GetString(r.Message.Value, 0, r.Message.Value.Length));
            Assert.Null(r.Message.Key);
            Assert.Equal(r.Message.Timestamp.Type, dr.Message.Timestamp.Type);
            Assert.Equal(r.Message.Timestamp.UnixTimestampMs, dr.Message.Timestamp.UnixTimestampMs);
        }

        private static DeliveryResult<Null, string> ProduceMessage(string topic, IProducer<Null, string> producer, string testString)
        {
            var result = producer.ProduceAsync(topic, new Message<Null, string> { Value = testString }).Result;
            Assert.NotNull(result?.Message);
            Assert.Equal(topic, result.Topic);
            Assert.NotEqual<long>(result.Offset, Offset.Unset);
            Assert.Equal(TimestampType.CreateTime, result.Message.Timestamp.Type);
            Assert.True(Math.Abs((DateTime.UtcNow - result.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
            Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            return result;
        }
    }
}
