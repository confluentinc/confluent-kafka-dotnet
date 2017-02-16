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

using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every Producer&lt;TKey,TValue&gt;.ProduceAsync method overload
    ///     that provides delivery reports via an IDeliveryHandler instance
    ///     (null key/value case)
    /// </summary>
    public static partial class Tests
    {
        class DeliveryHandler_SPN : IDeliveryHandler<Null, Null>
        {
            public DeliveryHandler_SPN(string topic)
            {
                Topic = topic;
            }

            public bool MarshalData { get { return true; } }

            public int Count { get; private set; }

            public string Topic { get; }

            public void HandleDeliveryReport(Message<Null, Null> dr)
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(0, dr.Partition);
                Assert.Equal(Topic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Null(dr.Key);
                Assert.Null(dr.Value);
                Count += 1;
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static void SerializingProducer_ProduceAsync_Null_DeliveryHandler(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var dh = new DeliveryHandler_SPN(topic);

            using (var producer = new Producer<Null, Null>(producerConfig, null, null))
            {
                producer.ProduceAsync(topic, null, null, 0, true, dh);
                producer.ProduceAsync(topic, null, null, 0, dh);
                producer.ProduceAsync(topic, null, null, true, dh);
                producer.ProduceAsync(topic, null, null, dh);
                producer.Flush();
            }

            Assert.Equal(4, dh.Count);
        }
    }
}
