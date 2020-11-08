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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {

        /// <summary>
        ///     Tests for GetWatermarkOffsets and QueryWatermarkOffsets.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void WatermarkOffsets(string bootstrapServers)
        {
            LogToFile("start WatermarkOffsets");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            var testString = "hello world";

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                DeliveryResult<Null, string> dr;
                using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
                {
                    dr = producer.ProduceAsync(topic.Name, new Message<Null, string> { Value = testString }).Result;
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10))); // this isn't necessary.
                }

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });
                    var record = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.NotNull(record.Message);

                    var getOffsets = consumer.GetWatermarkOffsets(dr.TopicPartition);
                    Assert.Equal(0, getOffsets.Low);
                    // the offset of the next message to be read.
                    Assert.Equal(dr.Offset + 1, getOffsets.High);

                    var queryOffsets = consumer.QueryWatermarkOffsets(dr.TopicPartition, TimeSpan.FromSeconds(20));
                    Assert.NotEqual(Offset.Unset, queryOffsets.Low);
                    Assert.Equal(getOffsets.High, queryOffsets.High);
                }
            }

            // Test empty topic case
            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                var wo = consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, 0), TimeSpan.FromSeconds(30));
                // Refer to WatermarkOffsets class documentation for more information.
                Assert.Equal(0, wo.Low);
                Assert.Equal(0, wo.High);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   WatermarkOffsets");
        }

    }
}
