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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Ensures that awaiting ProduceAsync does not deadlock and
        ///     some other basic things.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_ProduceAsync_Await_Serializing(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Await_Serializing");

            Func<Task> mthd = async () => 
            {
                using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                {
                    var dr = await producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<Null, string> { Value = "test string" });
                    Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
                    Assert.NotEqual(Offset.Unset, dr.Offset);
                }
            };

            mthd().Wait();
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await_Serializing");
        }

        /// <summary>
        ///     Ensures that awaiting ProduceAsync does not deadlock and
        ///     some other basic things (variant 2).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_ProduceAsync_Await_NonSerializing(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Await_NonSerializing");

            using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var dr = await producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                Assert.NotEqual(Offset.Unset, dr.Offset);
            }

            Assert.Equal(0, Library.HandleCount);

            LogToFile("end   Producer_ProduceAsync_Await_NonSerializing");
        }

        /// <summary>
        ///     Ensures that ProduceAsync throws when the DeliveryReport 
        ///     has an error (produced to non-existent partition).
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_ProduceAsync_Await_Throws(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Await_Throws");

            using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            {
                await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(
                    async () => 
                    {
                        await producer.ProduceAsync(
                            new TopicPartition(singlePartitionTopic, 42),
                            new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                        throw new Exception("unexpected exception");
                    });
            }
            
            // variation 2

            Func<Task> mthd = async () =>
            {
                using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
                {
                    var dr = await producer.ProduceAsync(
                        new TopicPartition(singlePartitionTopic, 1001),
                        new Message<byte[], byte[]> { Value = Encoding.UTF8.GetBytes("test string") });
                    throw new Exception("unexpected exception.");
                }
            };

            Assert.Throws<AggregateException>(() => { mthd().Wait(); });

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Await_Throws");
        }
    }
}
