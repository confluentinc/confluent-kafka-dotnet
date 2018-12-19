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
    public static partial class Tests
    {
        /// <summary>
        ///     Tests that log messages are received by OnLog on all Producer and Consumer variants.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void LogDelegate(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
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

            DeliveryResult dr;

            using (var producer = new Producer(producerConfig))
            {
                producer.OnLog += (_, m)
                    => logCount += 1;

                dr = producer.ProduceAsync(singlePartitionTopic, new Message { Value = Serializers.Utf8.Serialize("test value", true, null, null) }).Result;
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.OnLog += (_, m)
                    => logCount += 1;

                consumer.Assign(new TopicPartition(singlePartitionTopic, 0));
                
                consumer.Consume(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   LogDelegate");
        }

    }
}
