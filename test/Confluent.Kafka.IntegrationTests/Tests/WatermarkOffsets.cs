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
        ///     Tests for GetWatermarkOffsets and QueryWatermarkOffsets on producer and consumer.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void WatermarkOffsets(string bootstrapServers, string topic)
        {
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers }
            };

            var testString = "hello world";

            Message<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(topic, null, testString).Result;
                producer.Flush();

                var getOffsets = producer.GetWatermarkOffsets(new TopicPartition(topic, 0));

                // statistics.interval.ms is not set, so this should always be invalid.
                Assert.Equal(getOffsets.Low, Offset.Invalid);

                // no message has been consumed from broker (this is a producer), so this should always be invalid.
                Assert.Equal(getOffsets.High, Offset.Invalid);

                var queryOffsets = producer.QueryWatermarkOffsets(new TopicPartition(topic, 0));
                Assert.NotEqual(queryOffsets.Low, Offset.Invalid);
                Assert.NotEqual(queryOffsets.High, Offset.Invalid);

                // TODO: can anything be said about the high watermark offset c.f. dr.Offset?
                //       I have seen queryOffsets.High < dr.Offset and also queryOffsets.High = dr.Offset + 1.
                //       The former only once (or was I in error?). request.required.acks has a default value
                //       of 1, so with only one broker, I assume the former should never happen.
                Console.WriteLine($"Query Offsets: [{queryOffsets.Low} {queryOffsets.High}]. DR Offset: {dr.Offset}");
                Assert.True(queryOffsets.Low < queryOffsets.High);
            }

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "watermark-offset-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });
                Message msg;
                Assert.True(consumer.Consume(out msg, TimeSpan.FromSeconds(10)));

                var getOffsets = consumer.GetWatermarkOffsets(dr.TopicPartition);
                Assert.Equal(getOffsets.Low, Offset.Invalid);
                // the offset of the next message to be read.
                Assert.Equal((long)getOffsets.High, dr.Offset + 1);

                var queryOffsets = consumer.QueryWatermarkOffsets(dr.TopicPartition);
                Assert.NotEqual(queryOffsets.Low, Offset.Invalid);
                Assert.Equal(getOffsets.High, queryOffsets.High);
            }
        }

    }
}
