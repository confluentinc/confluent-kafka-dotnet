// Copyright 2018 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test of disabling marshaling of message headers.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Consumer_DisableHeaders(string bootstrapServers)
        {
            LogToFile("start Consumer_DisableHeaders");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                ConsumeResultFields = "timestamp,topic"
            };

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            DeliveryResult<byte[], byte[]> dr;
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                dr = producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]>
                    {
                        Value = Serializers.Utf8.Serialize("my-value", SerializationContext.Empty),
                        Headers = new Headers() { new Header("my-header", new byte[] { 42 }) }
                    }
                ).Result;
            }

            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetErrorHandler((_, e) => Assert.True(false, e.Reason))
                    .Build())
            {                    
                consumer.Assign(new TopicPartitionOffset[] { new TopicPartitionOffset(singlePartitionTopic, 0, dr.Offset) });

                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Null(record.Message.Headers);
                Assert.NotEqual(TimestampType.NotAvailable, record.Message.Timestamp.Type);
                Assert.NotEqual(0, record.Message.Timestamp.UnixTimestampMs);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_DisableHeaders");
        }

    }
}
