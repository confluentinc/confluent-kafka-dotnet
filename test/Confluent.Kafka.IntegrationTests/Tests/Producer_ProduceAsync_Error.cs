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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test <see cref="Producer.ProduceAsync" /> error cases.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_ProduceAsync_Error(string bootstrapServers)
        {
            LogToFile("start Producer_ProduceAsync_Error");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };


            // serialize case

            Task<DeliveryResult<string, string>> drt;
            using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
            {
                drt = producer.ProduceAsync(
                    new TopicPartition(partitionedTopic, 42),
                    new Message<string, string> { Key = "test key 0", Value = "test val 0" });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            Assert.Throws<AggregateException>(() => { drt.Wait(); });

            try
            {
                var dr = drt.Result;
            }
            catch (AggregateException e)
            {
                var inner = e.InnerException;
                Assert.IsType<ProduceException<string, string>>(inner);
                var dr = ((ProduceException<string, string>)inner).DeliveryResult;
                var err = ((ProduceException<string, string>)inner).Error;
                
                Assert.True(err.IsError);
                Assert.Equal(PersistenceStatus.NotPersisted, dr.Status);
                Assert.False(err.IsFatal);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.Equal(Offset.Unset, dr.Offset);
                Assert.True(dr.Partition == 42);
                Assert.Equal($"test key 0", dr.Message.Key);
                Assert.Equal($"test val 0", dr.Message.Value);
                Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
            }

            // byte[] case

            Task<DeliveryResult<byte[], byte[]>> drt2;
            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                drt2 = producer.ProduceAsync(
                    new TopicPartition(partitionedTopic, 42),
                    new Message<byte[], byte[]> { Key = new byte[] { 100 }, Value = new byte[] { 101 } });
                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            Assert.Throws<AggregateException>(() => { drt.Wait(); });

            try
            {
                var dr = drt2.Result;
            }
            catch (AggregateException e)
            {
                var inner = e.InnerException;
                Assert.IsType<ProduceException<byte[], byte[]>>(inner);
                var dr = ((ProduceException<byte[], byte[]>)inner).DeliveryResult;
                var err = ((ProduceException<byte[], byte[]>)inner).Error;
                
                Assert.True(err.IsError);
                Assert.False(err.IsFatal);
                Assert.Equal(partitionedTopic, dr.Topic);
                Assert.Equal(Offset.Unset, dr.Offset);
                Assert.True(dr.Partition == 42);
                Assert.Equal(new byte[] { 100 }, dr.Message.Key);
                Assert.Equal(new byte[] { 101 }, dr.Message.Value);
                Assert.Equal(TimestampType.NotAvailable, dr.Message.Timestamp.Type);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ProduceAsync_Error");
        }
    }
}
