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
using System.Threading.Tasks;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test reuse of the same object for multiple produce calls.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ReuseObjectInstance(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "partitioner", "murmur2_random" },
                { "message.send.max.retries", 0 },
                { "linger.ms", 100 }
            };

            var key = new byte[] { 1, 2, 3, 4 };
            var val = new byte[] { 5, 6, 7, 8 };

            using (var producer = new Producer(producerConfig))
            {
                Action<DeliveryReport> dh = null;
                var drs = new List<DeliveryReport>();

                // TODO: This fails when assertInDH == true. I have no idea why.
                bool assertInDH = false;
                if (assertInDH)
                {
                    int count = 0;
                    dh = (DeliveryReport dr) =>
                    {
                        switch (count) 
                        {
                            case 0: 
                                Assert.Equal(new byte[] { 1, 2, 3, 4 }, dr.Message.Key);
                                Assert.Equal(new byte[] { 5, 6, 7, 8 }, dr.Message.Value);
                                break;
                            case 1:
                                Assert.Equal(new byte[] { 1, 2 }, dr.Message.Key);
                                Assert.Equal(new byte[] { 5, 6, 7 }, dr.Message.Value);
                                break;
                            case 2:
                                Assert.Equal(new byte[] { 1, 2, 3 }, dr.Message.Key);
                                Assert.Equal(new byte[] { 6, 7, 8 }, dr.Message.Value);
                                break;
                            case 3:
                                Assert.Equal(new byte[] { 1 }, dr.Message.Key);
                                Assert.Equal(new byte[] { 7, 8 }, dr.Message.Value);
                                break;
                            default:
                                Assert.True(false);
                                break;
                        }

                        count += 1;
                    };
                }
                else
                {
                    dh = (DeliveryReport dr) => drs.Add(dr);
                }

                producer.Produce(dh, partitionedTopic, Partition.Any, key, 0, 4, val, 0, 4, Timestamp.Default, null);
                producer.Produce(dh, partitionedTopic, Partition.Any, key, 0, 2, val, 0, 3, Timestamp.Default, null);
                producer.Produce(dh, partitionedTopic, Partition.Any, key, 0, 3, val, 1, 3, Timestamp.Default, null);
                producer.Produce(dh, partitionedTopic, Partition.Any, key, 0, 1, val, 2, 2, Timestamp.Default, null);

                producer.Flush(TimeSpan.FromSeconds(10));

                if (!assertInDH)
                {
                    Assert.Equal(new byte[] { 1, 2, 3, 4 }, drs[0].Message.Key);
                    Assert.Equal(new byte[] { 5, 6, 7, 8 }, drs[0].Message.Value);
                    Assert.Equal(new byte[] { 1, 2 }, drs[1].Message.Key);
                    Assert.Equal(new byte[] { 5, 6, 7 }, drs[1].Message.Value);
                    Assert.Equal(new byte[] { 1, 2, 3 }, drs[2].Message.Key);
                    Assert.Equal(new byte[] { 6, 7, 8 }, drs[2].Message.Value);
                    Assert.Equal(new byte[] { 1 }, drs[3].Message.Key);
                    Assert.Equal(new byte[] { 7, 8 }, drs[3].Message.Value);
                }
            }
        }
    }
}
