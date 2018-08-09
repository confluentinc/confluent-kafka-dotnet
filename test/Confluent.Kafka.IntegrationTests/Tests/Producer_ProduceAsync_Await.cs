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
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Ensures that awaiting ProduceAsync does not deadlock.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_ProduceAsync_Await(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_ProduceAsync_Await");

            Func<Task> mthd = async () => 
            {
                using (var producer = new Producer<Null, string>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, new StringSerializer(Encoding.UTF8)))
                {
                    var dr = await producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "test string" });
                    Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                    producer.Flush(TimeSpan.FromSeconds(10));
                }
            };

            mthd().Wait();
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await");
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public static async Task Producer_ProduceAsync_Await2(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_ProduceAsync_Await2");

            using (var producer = new Producer<Null, string>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, new StringSerializer(Encoding.UTF8)))
            {
                var dr = await producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = "test string" });
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await2");
        }

        /// <summary>
        ///     Ensures that ProduceAsync throws when the DeliveryReport has an error (produced to non-existant partition).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static async Task Producer_ProduceAsync_Await3(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_ProduceAsync_Await3");

            using (var producer = new Producer<Null, string>(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }, null, new StringSerializer(Encoding.UTF8)))
            {
                await Assert.ThrowsAsync<ProduceMessageException<Null, string>>(
                    async () => await producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 42), new Message<Null, string> { Value = "test string" }));
            }
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await3");
        }
    }
}
