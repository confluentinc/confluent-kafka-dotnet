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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every <see cref="Producer.BeginProduce" /> method overload.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_BeginProduce(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_BeginProduce");

            var producerConfig = new ProducerConfig
            { 
                BootstrapServers = bootstrapServers,
                EnableIdempotence = true
            };


            // serializer case.

            int count = 0;
            Action<DeliveryReport<string, string>> dh = (DeliveryReport<string, string> dr) =>
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal((Partition)0, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Equal($"test key {count}", dr.Message.Key);
                Assert.Equal($"test val {count}", dr.Message.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                count += 1;
            };

            using (var producer = new Producer<string, string>(producerConfig))
            {
                producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0), 
                    new Message<string, string> { Key = "test key 0", Value = "test val 0" }, dh);

                producer.BeginProduce(
                    singlePartitionTopic,
                    new Message<string, string> { Key = "test key 1", Value = "test val 1" }, dh);

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(2, count);


            // byte[] case.

            count = 0;
            Action<DeliveryReport> dh2 = (DeliveryReport dr) =>
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal((Partition)0, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Equal($"test key {count + 42}", Encoding.UTF8.GetString(dr.Message.Key));
                Assert.Equal($"test val {count + 42}", Encoding.UTF8.GetString(dr.Message.Value));
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                count += 1;
            };

            using (var producer = new Producer(producerConfig))
            {
                producer.BeginProduce(
                    new TopicPartition(singlePartitionTopic, 0), 
                    new Message { Key = Encoding.UTF8.GetBytes("test key 42"), Value = Encoding.UTF8.GetBytes("test val 42") }, dh2);

                producer.BeginProduce(
                    singlePartitionTopic, 
                    new Message { Key = Encoding.UTF8.GetBytes("test key 43"), Value = Encoding.UTF8.GetBytes("test val 43") }, dh2);

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(2, count);


            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_BeginProduce");
        }
    }
}
