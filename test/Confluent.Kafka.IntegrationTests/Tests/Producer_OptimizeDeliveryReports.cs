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

using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test where no fields are enabled in delivery report.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public async void Producer_OptimizeDeliveryReports(string bootstrapServers)
        {
            LogToFile("start Producer_OptimizeDeliveryReports");

            byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            var producerConfig = new ProducerConfig
            { 
                BootstrapServers = bootstrapServers,
                DeliveryReportFields = "none"
            };


            // serializing case. 

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                var dr = await producer.ProduceAsync(
                    singlePartitionTopic, 
                    new Message<byte[], byte[]> 
                    { 
                        Key = TestKey, 
                        Value = TestValue, 
                        Headers = new Headers() { new Header("my-header", new byte[] { 42 }) } 
                    }
                );
                Assert.Equal(TimestampType.NotAvailable, dr.Timestamp.Type);
                Assert.Equal(0, dr.Timestamp.UnixTimestampMs);
                Assert.Null(dr.Value);
                Assert.Null(dr.Key);
                Assert.Null(dr.Headers);
            }


            // byte[] case. 

            using (var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build())
            {
                var dr = await producer.ProduceAsync(
                    singlePartitionTopic, 
                    new Message<byte[], byte[]>
                    { 
                        Key = TestKey, 
                        Value = TestValue, 
                        Headers = new Headers() { new Header("my-header", new byte[] { 42 }) } 
                    }
                );
                Assert.Equal(TimestampType.NotAvailable, dr.Timestamp.Type);
                Assert.Equal(0, dr.Timestamp.UnixTimestampMs);
                Assert.Null(dr.Value);
                Assert.Null(dr.Key);
                Assert.Null(dr.Headers);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_OptimizeDeliveryReports");
        }
    }
}
