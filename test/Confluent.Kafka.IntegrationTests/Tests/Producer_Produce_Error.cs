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
    ///     Tests <see cref="Producer.Produce" /> error cases.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_Error(string bootstrapServers)
        {
            LogToFile("start Producer_Produce_Error");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };


            // serializer case.

            int count = 0;
            Action<DeliveryReport<Null, String>> dh = (DeliveryReport<Null, String> dr) => 
            {
                Assert.Equal(ErrorCode.Local_UnknownPartition, dr.Error.Code);
                Assert.False(dr.Error.IsFatal);
                Assert.Equal((Partition)1, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.Equal(Offset.Unset, dr.Offset);
                Assert.Null(dr.Message.Key);
                Assert.Equal("test", dr.Message.Value);
                Assert.Equal(PersistenceStatus.NotPersisted, dr.Status);
                Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
                count += 1;
            };

            using (var producer =
                new ProducerBuilder<Null, String>(producerConfig)
                    .SetKeySerializer(Serializers.Null)
                    .SetValueSerializer(Serializers.Utf8)
                    .Build())
            {
                producer.Produce(new TopicPartition(singlePartitionTopic, 1), new Message<Null, String> { Value = "test" }, dh);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(1, count);


            // byte[] case.

            count = 0;
            Action<DeliveryReport<byte[], byte[]>> dh2 = (DeliveryReport<byte[], byte[]> dr) =>
            {
                Assert.Equal(ErrorCode.Local_UnknownPartition, dr.Error.Code);
                Assert.Equal((Partition)42, dr.Partition);
                Assert.Equal(singlePartitionTopic, dr.Topic);
                Assert.Equal(Offset.Unset, dr.Offset);
                Assert.Equal(new byte[] { 11 }, dr.Message.Key);
                Assert.Null(dr.Message.Value);
                Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
                count += 1;
            };

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                producer.Produce(new TopicPartition(singlePartitionTopic, 42), new Message<byte[], byte[]> { Key = new byte[] { 11 }}, dh2);
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            Assert.Equal(1, count);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Produce_Error");
        }
    }
}
