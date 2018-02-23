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
using System.Text;
using Xunit;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every obsolete ProduceAsync method.
    /// </summary>
    public static partial class Tests
    {
        class ObsoleteTestDeliveryHandler : IDeliveryHandler
        {
            public bool MarshalData
                => true;

            public bool MarshalHeaders
                => true;

            public void HandleDeliveryReport(Message dr)
                => drs.Add(dr);

            public List<Message> drs = new List<Message>();
        }

        class ObsoleteTestDeliveryHandler_2 : IDeliveryHandler<string, string>
        {
            public bool MarshalData
                => true;

            public bool MarshalHeaders
                => true;

            public void HandleDeliveryReport(Message<string, string> dr)
                => drs.Add(dr);

            public List<Message<string, string>> drs = new List<Message<string, string>>();
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ObsoleteMethods(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "partitioner", "murmur2_random" },
                { "message.send.max.retries", 0 }
            };

            var key = new byte[] { 1, 2, 3, 4 };
            var val = new byte[] { 5, 6, 7, 8 };

            var dh = new ObsoleteTestDeliveryHandler();
            var dh2 = new ObsoleteTestDeliveryHandler_2();

            var drs = new List<Task<Message>>();
            using (var producer = new Producer(producerConfig))
            {
                var dr = producer.ProduceAsync(partitionedTopic, key, 1, 2, val, 2, 1).Result;
                Assert.Equal(new byte[] { 2, 3 }, dr.Key);
                Assert.Equal(new byte[] { 7 }, dr.Value);

                dr = producer.ProduceAsync(partitionedTopic, key, 2, 1, val, 1, 2, 1).Result;
                Assert.Equal(new byte[] { 3 }, dr.Key);
                Assert.Equal(new byte[] { 6, 7 }, dr.Value);
                Assert.Equal(1, (int)dr.Partition);

                dr = producer.ProduceAsync(partitionedTopic, key, 1, 1, val, 1, 1, 1, true).Result;
                Assert.Equal(new byte[] { 2 }, dr.Key);
                Assert.Equal(new byte[] { 6 }, dr.Value);
                Assert.Equal(1, (int)dr.Partition);

                dr = producer.ProduceAsync(partitionedTopic, key, 0, 1, val, 0, 1, true).Result;
                Assert.Equal(new byte[] { 1 }, dr.Key);
                Assert.Equal(new byte[] { 5 }, dr.Value);  

                producer.ProduceAsync(partitionedTopic, key, val, dh);
                producer.ProduceAsync(partitionedTopic, key, 2, 2, val, 0, 3, dh);
                producer.ProduceAsync(partitionedTopic, key, 1, 3, val, 1, 3, 1, dh);
                producer.ProduceAsync(partitionedTopic, key, 0, 1, val, 2, 2, 1, true, dh);
            
                producer.Flush(TimeSpan.FromSeconds(10));

                Assert.Equal(new byte[] { 1, 2, 3, 4 }, dh.drs[0].Key);
                Assert.Equal(new byte[] { 5, 6, 7, 8 }, dh.drs[0].Value);
                Assert.Equal(new byte[] { 3, 4 }, dh.drs[1].Key);
                Assert.Equal(new byte[] { 5, 6, 7 }, dh.drs[1].Value);
                Assert.Equal(new byte[] { 2, 3, 4 }, dh.drs[2].Key);
                Assert.Equal(new byte[] { 6, 7, 8 }, dh.drs[2].Value);
                Assert.Equal(new byte[] { 1 }, dh.drs[3].Key);
                Assert.Equal(new byte[] { 7, 8 }, dh.drs[3].Value);
            }

            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                var dr = producer.ProduceAsync(partitionedTopic, "hello", "world", 0, true).Result;
                Assert.Equal("hello", dr.Key);
                Assert.Equal("world", dr.Value);

                dr = producer.ProduceAsync(partitionedTopic, "hello1", "world1", 0).Result;
                Assert.Equal("hello1", dr.Key);
                Assert.Equal("world1", dr.Value);

                dr = producer.ProduceAsync(partitionedTopic, "hello2", "world2", true).Result;
                Assert.Equal("hello2", dr.Key);
                Assert.Equal("world2", dr.Value);

                producer.ProduceAsync(partitionedTopic, "hello3", "world3", dh2);
                producer.ProduceAsync(partitionedTopic, "hello4", "world4", true, dh2);
                producer.ProduceAsync(partitionedTopic, "hello5", "world5", 1, dh2);
                producer.ProduceAsync(partitionedTopic, "hello6", "world6", 1, true, dh2);

                producer.Flush(TimeSpan.FromSeconds(10));

                Assert.Equal("hello3", dh2.drs[0].Key);
                Assert.Equal("world3", dh2.drs[0].Value);
                Assert.Equal("hello4", dh2.drs[1].Key);
                Assert.Equal("world4", dh2.drs[1].Value);
                Assert.Equal("hello5", dh2.drs[2].Key);
                Assert.Equal("world5", dh2.drs[2].Value);
                Assert.Equal("hello6", dh2.drs[3].Key);
                Assert.Equal("world6", dh2.drs[3].Value);

                // This test intermittently fails, with eg:
                // 
                // [xUnit.net 00:00:05.2823790]     Confluent.Kafka.IntegrationTests.Tests.Producer_ObsoleteMethods(bootstrapServers: "localhost:9092", singlePartitionTopic: "test-topic-1", partitionedTopic: "test-topic-2") [FAIL]
                // [xUnit.net 00:00:05.2839470]       Assert.Equal() Failure
                // [xUnit.net 00:00:05.2840770]                      ↓ (pos 5)
                // [xUnit.net 00:00:05.2841330]       Expected: hello5
                // [xUnit.net 00:00:05.2842020]       Actual:   hello6
                // [xUnit.net 00:00:05.2842390]                      ↑ (pos 5)
            }
        }
    }
}
