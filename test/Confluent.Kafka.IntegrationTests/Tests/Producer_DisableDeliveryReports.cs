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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test dotnet.producer.enable.delivery.reports == true
    ///     results in no delivery report.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_DisableDeliveryReports(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "dotnet.producer.enable.delivery.reports", false },
                // the below are just tests that the property is recognized. the functionality is not tested.
                { "dotnet.producer.block.if.queue.full", false }, 
                { "dotnet.producer.enable.background.poll", true },
                { "dotnet.producer.enable.deivery.report.header.marshaling", true },
                { "dotnet.producer.enable.deivery.report.data.marshaling", true}
            };

            int count = 0;
            using (var producer = new Producer(producerConfig))
            {
                producer.Produce(
                    (DeliveryReport dr) => count += 1,
                    singlePartitionTopic, 0,
                    TestKey, 0, TestKey.Length,
                    TestValue, 0, TestValue.Length,
                    Timestamp.Default, null
                );

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(0, count);
        }
    }
}
