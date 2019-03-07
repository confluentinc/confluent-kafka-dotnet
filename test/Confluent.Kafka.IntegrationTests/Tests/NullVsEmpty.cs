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
        ///     Test that null and byte[0] keys and values are produced / consumed
        ///     as expected.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void NullVsEmpty(string bootstrapServers)
        {
            LogToFile("start NullVsEmpty");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers
            };

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            DeliveryResult<byte[], byte[]> dr;
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                // Assume that all these produce calls succeed.
                dr = producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = null, Value = null }).Result;
                producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = null, Value = new byte[0] {} }).Wait();
                producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = new byte[0] {}, Value = null }).Wait();
                producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = new byte[0] {}, Value = new byte[0] {} }).Wait();
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Assign(new List<TopicPartitionOffset>() { dr.TopicPartitionOffset });

                var record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Null(record.Message.Key);
                Assert.Null(record.Message.Value);

                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Null(record.Message.Key);
                Assert.Equal(record.Message.Value, new byte[0]);

                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(record.Message.Key, new byte[0]);
                Assert.Null(record.Message.Value);

                record = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(record.Message);
                Assert.Equal(record.Message.Key, new byte[0]);
                Assert.Equal(record.Message.Value, new byte[0]);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   NullVsEmpty");
        }

    }
}
