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
using System.Threading.Tasks;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every Producer&lt;TKey,TValue&gt;.ProduceAsync method overload
    ///     that provides delivery reports via a Task.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void SerializingProducer_ProduceAsync_Task(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers }
            };

            var drs = new List<Task<DeliveryReport<string, string>>>();
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                drs.Add(producer.ProduceAsync(
                    new TopicPartition(partitionedTopic, 1),
                    new Message<string, string> { Key = "test key 0", Value = "test val 0" }));
                drs.Add(producer.ProduceAsync(partitionedTopic, new Message<string, string> { Key = "test key 1", Value = "test val 1" }));
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            for (int i=0; i<2; ++i)
            {
                var dr = drs[i].Result;
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.True(dr.Partition == 0 || dr.Partition == 1);
                Assert.Equal($"test key {i}", dr.Message.Key);
                Assert.Equal($"test val {i}", dr.Message.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
            }

            Assert.Equal((Partition)1, drs[0].Result.Partition);
        }
    }
}
