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
    /// <summary>
    ///     Test every Producer.ProduceAsync method overload that provides
    ///     delivery reports via an IDeliveryHandler instance.
    /// </summary>
    public static partial class Tests
    {
        class DeliveryHandler_P : IDeliveryHandler
        {
            public static byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            public static byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            public DeliveryHandler_P(string topic)
            {
                Topic = topic;
            }

            public bool MarshalData { get { return true; } }

            public int Count { get; private set; }

            public string Topic { get; }

            public void HandleDeliveryReport(Message dr)
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal((Partition)0, dr.Partition);
                Assert.Equal(Topic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Equal(TimestampType.CreateTime, dr.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Timestamp.UtcDateTime).TotalMinutes) < 1.0);

                if (Count < 5)
                {
                    Assert.Equal(TestKey, dr.Key);
                    Assert.Equal(TestValue, dr.Value);
                }
                else
                {
                    Assert.Equal(new byte[] { 2, 3 }, dr.Key);
                    Assert.Equal(new byte[] { 7 }, dr.Value);
                }

                Count += 1;
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ProduceAsync_DeliveryHandler(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers }
            };

            var dh = new DeliveryHandler_P(singlePartitionTopic);

            using (var producer = new Producer(producerConfig))
            {
                producer.Produce(
                    singlePartitionTopic, 0,
                    DeliveryHandler_P.TestKey, 0, DeliveryHandler_P.TestKey.Length,
                    DeliveryHandler_P.TestValue, 0, DeliveryHandler_P.TestValue.Length,
                    Timestamp.Default, null, dh
                );

                producer.Produce(
                    singlePartitionTopic, 0,
                    DeliveryHandler_P.TestKey, 0, DeliveryHandler_P.TestKey.Length,
                    DeliveryHandler_P.TestValue, 0, DeliveryHandler_P.TestValue.Length,
                    Timestamp.Default, null, dh
                );

                producer.Produce(
                    singlePartitionTopic, 0,
                    DeliveryHandler_P.TestKey, 0, DeliveryHandler_P.TestKey.Length,
                    DeliveryHandler_P.TestValue, 0, DeliveryHandler_P.TestValue.Length,
                    Timestamp.Default, null, dh
                );

                producer.Produce(
                    singlePartitionTopic,
                    DeliveryHandler_P.TestKey,
                    DeliveryHandler_P.TestValue,
                    dh
                );

                producer.Produce(
                    singlePartitionTopic, 
                    DeliveryHandler_P.TestKey, 
                    DeliveryHandler_P.TestValue, 
                    dh
                );

                producer.Produce(
                    singlePartitionTopic, Partition.NotSpecified,
                    DeliveryHandler_P.TestKey, 1, DeliveryHandler_P.TestKey.Length-2,
                    DeliveryHandler_P.TestValue, 2, DeliveryHandler_P.TestValue.Length-3,
                    Timestamp.Default, null, dh
                );

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(6, dh.Count);
        }
    }
}
