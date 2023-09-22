// Copyright 2018 Confluent Inc.
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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test a variety of cases where a producer is constructed
    ///     using the handle from another producer.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Binary_Handles(string bootstrapServers)
        {
            LogToFile("start Producer_Binary_Handles");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const int nativeLength = 12;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                using (var producer2 = new DependentProducerBuilder(producer1.Handle).Build())
                using (var producer3 = new DependentProducerBuilder(producer1.Handle).Build())
                {
                    var r1 = producer1.ProduceAsync(topic.Name, new ReadOnlySpan<byte>(new byte[] { 42 }), new ReadOnlySpan<byte>(new byte[] { 33 })).Result;
                    Assert.Equal(0, r1.Offset);

                    Span<byte> stackKey = stackalloc byte[Encoding.UTF8.GetByteCount("hello")];
                    Span<byte> stackValue = stackalloc byte[Encoding.UTF8.GetByteCount("world")];
                    Encoding.UTF8.GetBytes("hello", stackKey);
                    Encoding.UTF8.GetBytes("world", stackValue);
                    var r2 = producer2.ProduceAsync(topic.Name, stackKey, stackValue).Result;

                    
                    var native = Marshal.AllocHGlobal(nativeLength);
                    Span<byte> nativeValue;
                    unsafe
                    {
                        nativeValue = new Span<byte>(native.ToPointer(), nativeLength);
                    }
                    byte data = 0;
                    for (var index = 0; index < nativeValue.Length; index++)
                        nativeValue[index] = data++;

                    try
                    {
                        _ = producer3.ProduceAsync(topic.Name, new byte[] { 40 }, nativeValue).Result;
                    }
                    finally
                    {
                        Marshal.FreeHGlobal(native);
                    }
                }

                var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 42 }, r1.Message.Key);
                    Assert.Equal(new byte[] { 33 }, r1.Message.Value);
                    Assert.Equal(0, r1.Offset);
                }

                using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 1));
                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", r2.Message.Key);
                    Assert.Equal("world", r2.Message.Value);
                    Assert.Equal(1, r2.Offset);
                }

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 2));
                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 40 }, r3.Message.Key);
                    Assert.Equal(Enumerable.Range(0, nativeLength).Select(x => (byte)x).ToArray(), r3.Message.Value);
                    Assert.Equal(2, r3.Offset);
                }

            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Binary_Handles");
        }
    }
}