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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that log messages are received by OnLog on all Producer and Consumer variants.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void LogDelegate(string bootstrapServers)
        {
            LogToFile("start LogDelegate");

            var logCount = 0;

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                Debug = "all"
            };

            DeliveryResult<byte[], byte[]> dr;

            using (var producer =
                new ProducerBuilder<byte[], byte[]>(producerConfig)
                    .SetLogHandler((_, m) => logCount += 1)
                    .Build())
            {
                dr = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Value = Serializers.Utf8.Serialize("test value", SerializationContext.Empty) }).Result;
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var consumer =
                new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                    .SetLogHandler((_, m) => logCount += 1)
                    .Build())
            {
                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                consumer.Consume(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var adminClient =
                new AdminClientBuilder(adminConfig)
                    .SetLogHandler((_, m) => logCount += 1)
                    .Build())
            {
                adminClient.GetMetadata(TimeSpan.FromSeconds(1));
            }
            Assert.True(logCount > 0);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   LogDelegate");
        }

    }
}
