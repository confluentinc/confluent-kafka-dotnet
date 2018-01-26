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
    ///     Test every Producer.ProduceAsync method overload that provides
    ///     delivery reports via a Task
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ProduceAsync_Task(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers }
            };

            var key = new byte[] { 1, 2, 3, 4 };
            var val = new byte[] { 5, 6, 7, 8 };

            var drs = new List<Task<Message>>();
            using (var producer = new Producer(producerConfig))
            {
                drs.Add(producer.ProduceAsync(new Message(partitionedTopic, 1, Offset.Invalid, key, val, Timestamp.Default, null, null)));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, val));
                drs.Add(producer.ProduceAsync(partitionedTopic, 1, key, 0, key.Length, val, 0, val.Length, Timestamp.Default, null));
                drs.Add(producer.ProduceAsync(partitionedTopic, 0, key, 1, key.Length-1, val, 2, val.Length - 2, Timestamp.Default, null));
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            for (int i=0; i<3; ++i)
            {
                var dr = drs[i].Result;
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.True(dr.Partition == 0 || dr.Partition == 1);
                Assert.Equal(key, dr.Key);
                Assert.Equal(val, dr.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
            }

            Assert.Equal(new byte[] { 2, 3, 4 }, drs[3].Result.Key);
            Assert.Equal(new byte[] { 7, 8 }, drs[3].Result.Value);

            Assert.Equal((Partition)1, drs[0].Result.Partition);
            Assert.Equal((Partition)1, drs[2].Result.Partition);
        }
    }
}
