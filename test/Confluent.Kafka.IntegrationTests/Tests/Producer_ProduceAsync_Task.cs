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
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var key = new byte[] { 1, 2, 3, 4 };
            var val = new byte[] { 5, 6, 7, 8 };

            var drs = new List<Task<Message>>();
            using (var producer = new Producer(producerConfig))
            {
                drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, 1, true));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, 1));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length, true));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, 0, key.Length, val, 0, val.Length));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, val));
                drs.Add(producer.ProduceAsync(partitionedTopic, key, 1, key.Length-1, val, 2, val.Length-2));
                producer.Flush();
            }

            for (int i=0; i<5; ++i)
            {
                var dr = drs[i].Result;
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.NotEqual(Offset.Invalid, dr.Offset);
                Assert.True(dr.Partition == 0 || dr.Partition == 1);
                Assert.Equal(key, dr.Key);
                Assert.Equal(val, dr.Value);
            }

            Assert.Equal(new byte[] { 2, 3, 4 }, drs[5].Result.Key);
            Assert.Equal(new byte[] { 7, 8 }, drs[5].Result.Value);

            Assert.Equal(1, drs[0].Result.Partition);
            Assert.Equal(1, drs[1].Result.Partition);
        }
    }
}
