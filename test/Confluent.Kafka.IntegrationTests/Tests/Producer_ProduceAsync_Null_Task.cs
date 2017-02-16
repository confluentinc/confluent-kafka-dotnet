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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every Producer.ProduceAsync method overload that provides
    ///     delivery reports via a Task
    ///     (null key/value case)
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ProduceAsync_Null_Task(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var drs = new List<Task<Message>>();
            using (var producer = new Producer(producerConfig))
            {
                drs.Add(producer.ProduceAsync(partitionedTopic, null, 0, 0, null, 0, 0, 1, true));
                drs.Add(producer.ProduceAsync(partitionedTopic, null, 0, 0, null, 0, 0, 1));
                drs.Add(producer.ProduceAsync(partitionedTopic, null, 0, 0, null, 0, 0, true));
                drs.Add(producer.ProduceAsync(partitionedTopic, null, 0, 0, null, 0, 0));
                drs.Add(producer.ProduceAsync(partitionedTopic, null, null));
                Assert.Throws<ArgumentException>(() => { producer.ProduceAsync(partitionedTopic, null, 8, 100, null, -33, int.MaxValue); });
                producer.Flush();
            }

            for (int i=0; i<5; ++i)
            {
                var dr = drs[i].Result;
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.True(dr.Partition == 0 || dr.Partition == 1);
                Assert.Null(dr.Key);
                Assert.Null(dr.Value);
            }

            Assert.Equal(1, drs[0].Result.Partition);
            Assert.Equal(1, drs[1].Result.Partition);
        }
    }
}
