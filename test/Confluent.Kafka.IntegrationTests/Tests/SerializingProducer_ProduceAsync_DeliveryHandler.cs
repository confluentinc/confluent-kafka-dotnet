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
    /// <summary>
    ///     Test every Producer&lt;TKey,TValue&gt;.ProduceAsync method overload
    ///     that provides delivery reports via an IDeliveryHandler instance.
    /// </summary>
    public static partial class Tests
    {
        class DeliveryHandler_SP : IDeliveryHandler<string, string>
        {
            public DeliveryHandler_SP(string topic)
            {
                Topic = topic;
            }

            public bool MarshalData { get { return true; } }

            public int Count { get; private set; }

            public string Topic { get; }

            public void HandleDeliveryReport(Message<string, string> dr)
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(0, dr.Partition);
                Assert.Equal(Topic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Equal($"test key {Count}", dr.Key);
                Assert.Equal($"test val {Count}", dr.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                Count += 1;
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static void SerializingProducer_ProduceAsync_DeliveryHandler(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true }
            };

            var dh = new DeliveryHandler_SP(topic);

            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                producer.ProduceAsync(topic, "test key 0", "test val 0", 0, true, dh);
                producer.ProduceAsync(topic, "test key 1", "test val 1", 0, dh);
                producer.ProduceAsync(topic, "test key 2", "test val 2", true, dh);
                producer.ProduceAsync(topic, "test key 3", "test val 3", dh);
                producer.Flush();
            }

            Assert.Equal(4, dh.Count);
        }
    }
}
