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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test dotnet.producer.enable.delivery.reports == true
    ///     results in no delivery report.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public async static void Producer_OptimizeDeliveryReports(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "dotnet.producer.enable.delivery.report.headers", false },
                { "dotnet.producer.enable.delivery.report.timestamps", false },
                { "dotnet.producer.enable.delivery.report.keys", false },
                { "dotnet.producer.enable.delivery.report.values", false },
            };

            using (var producer = new Producer<byte[], byte[]>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer()))
            {
                var dr = await producer.ProduceAsync(
                    singlePartitionTopic, 
                    new Message<byte[], byte[]> 
                    { 
                        Key = TestKey, 
                        Value = TestValue, 
                        Headers = new Headers() { new Header("my-header", new byte[] { 42 }) } 
                    }
                );
                Assert.Equal(TimestampType.NotAvailable, dr.Timestamp.Type);
                Assert.Equal(0, dr.Timestamp.UnixTimestampMs);
                Assert.Null(dr.Value);
                Assert.Null(dr.Key);
                Assert.Null(dr.Headers);
            }
        }
    }
}
