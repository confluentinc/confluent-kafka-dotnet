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
    ///     Ensures that awaiting ProduceAsync does not deadlock.
    /// </summary>
    public static partial class Tests
    {
        public static async Task Producer_ProduceAsync_Await_Task(Dictionary<string, object> config, string topic)
        {
            using (var producer = new Producer(config))
            {
                var dr = await producer.ProduceAsync(topic, new byte[] {42}, new byte[] {44});
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ProduceAsync_Await(string bootstrapServers, string topic, string partitionedTopic)
        {
            var producerConfig = new Dictionary<string, object> 
            { 
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true }
            };

            var task = Producer_ProduceAsync_Await_Task(producerConfig, topic);
            task.Wait();
        }
    }
}
