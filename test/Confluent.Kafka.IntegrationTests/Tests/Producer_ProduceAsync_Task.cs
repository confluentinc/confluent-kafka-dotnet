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
        public static void Producer_ProduceAsync_Task(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true }
            };

            var key = new byte[] { 1, 2, 3, 4 };
            var val = new byte[] { 5, 6, 7, 8 };
            var now = DateTime.UtcNow;

            var drs = new List<Task<Message>>();
            using (var producer = new Producer(producerConfig))
            {
                /* 00 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, 1, true));
                /* 01 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, 1));
                /* 02 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, true));
                /* 03 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length));
                /* 04 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, val));
                /* 05 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, 1, key.Length-1, val, 2, val.Length-2));
                /* 06 */ drs.Add(producer.ProduceAsync(partitionedTopic, key, val, now));
                producer.Flush();
            }

            for(int i=0; i<drs.Count; i++)
            {
                var dr = drs[i].Result;
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                if (i == 0 || i == 1)
                {
                    Assert.True(dr.Partition == 1);
                }
                else
                {
                    Assert.True(dr.Partition == 0 || dr.Partition == 1);
                }

                if (i != 5)
                {
                    Assert.Equal(key, dr.Key);
                    Assert.Equal(val, dr.Value);
                }
                else
                {
                    Assert.Equal(new byte[] { 2, 3, 4 }, dr.Key);
                    Assert.Equal(new byte[] { 7, 8 }, dr.Value);
                }
                Assert.Equal(TimestampType.CreateTime, dr.Timestamp.Type);

                if (i == 6)
                {
                    //Datetime are not equal - ticks is more precise than unix timestamp
                    Assert.True(Math.Abs((now - dr.Timestamp.DateTime).TotalMilliseconds) <= 1.0);
                }
                else
                {
                    Assert.True(Math.Abs((DateTime.UtcNow - dr.Timestamp.DateTime).TotalMinutes) < 1.0);
                }
            }

        }
    }
}
