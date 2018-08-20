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
        ///     Tests that log messages are received by OnLog on all Producer and Consumer variants.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void LogDelegate(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start LogDelegate");

            var logCount = 0;
            Action<LogMessage> logger = (LogMessage m) => logCount += 1;

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", bootstrapServers },
                { "debug", "all" },
                { "log_cb", logger }
            };

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "debug", "all" },
                { "log_cb", logger }
            };

            DeliveryReport<Null, string> dr;

            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                dr = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "test value" }).Result;
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            Assert.True(logCount > 0);

            logCount = 0;
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Consume(TimeSpan.FromMilliseconds(100));
            }
            Assert.True(logCount > 0);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   LogDelegate");
        }

    }
}
