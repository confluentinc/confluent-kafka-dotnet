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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every <see cref="Producer.Produce" /> method overload (null case).
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_Null(string bootstrapServers)
        {
            LogToFile("start Producer_Produce_Null");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };


            // serializer case. 

            int count = 0;
            Action<DeliveryReport<Null, Null>> dh = (DeliveryReport<Null, Null> dr) => 
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(PersistenceStatus.Persisted, dr.Status);
                Assert.False(dr.Error.IsFatal);
                Assert.Equal((Partition)0, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Null(dr.Message.Key);
                Assert.Null(dr.Message.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                count += 1;
            };

            using (var producer = new ProducerBuilder<Null, Null>(producerConfig).Build())
            {
                producer.Produce(new TopicPartition(singlePartitionTopic, 0), new Message<Null, Null> {}, dh);
                producer.Produce(singlePartitionTopic, new Message<Null, Null> {}, dh);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(2, count);


            // byte[] case.

            count = 0;
            Action<DeliveryReport<byte[], byte[]>> dh2 = (DeliveryReport<byte[], byte[]> dr) =>
            {
                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal((Partition)0, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.True(dr.Offset >= 0);
                Assert.Null(dr.Message.Key);
                Assert.Null(dr.Message.Value);
                Assert.Equal(TimestampType.CreateTime, dr.Message.Timestamp.Type);
                Assert.True(Math.Abs((DateTime.UtcNow - dr.Message.Timestamp.UtcDateTime).TotalMinutes) < 1.0);
                count += 1;
            };

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                producer.Produce(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> {}, dh2);
                producer.Produce(singlePartitionTopic, new Message<byte[], byte[]> {}, dh2);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(2, count);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Produce_Null");
        }
    }
}
