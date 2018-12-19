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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test dotnet.producer.enable.delivery.reports == true
    ///     results in no delivery report.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_DisableDeliveryReports(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_DisableDeliveryReports");

            byte[] TestKey = new byte[] { 1, 2, 3, 4 };
            byte[] TestValue = new byte[] { 5, 6, 7, 8 };

            var producerConfig = new ProducerConfig
            { 
                BootstrapServers = bootstrapServers,
                EnableDeliveryReports = false,
                // the below are just a few extra tests that the property is recognized (all 
                // set to defaults). the functionality is not tested.
                EnableBackgroundPoll = true,
                DeliveryReportFields = "all"
            };

            // If delivery reports are disabled:
            //   1. callback functions should never called, even if specified.
            //   2. specifying no delivery report handlers is valid.
            //   3. tasks should complete immediately.
            int count = 0;
            using (var producer = new Producer(producerConfig))
            {
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Key = TestKey, Value = TestValue }, (DeliveryReport<byte[], byte[]> dr) => count += 1);
                producer.BeginProduce(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = TestKey, Value = TestValue });
                producer.BeginProduce(singlePartitionTopic, new Message<byte[], byte[]> { Key = TestKey, Value = TestValue });
                producer.BeginProduce(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = TestKey, Value = TestValue });

                var drTask = producer.ProduceAsync(singlePartitionTopic, new Message<byte[], byte[]> { Key = TestKey, Value = TestValue });
                Assert.True(drTask.IsCompleted);
                Assert.Equal(Offset.Invalid, drTask.Result.Offset);
                Assert.Equal(Partition.Any, drTask.Result.Partition);
                Assert.Equal(singlePartitionTopic, drTask.Result.Topic);
                Assert.Equal(TestKey, drTask.Result.Message.Key);
                Assert.Equal(TestValue, drTask.Result.Message.Value);

                drTask = producer.ProduceAsync(new TopicPartition(singlePartitionTopic, 0), new Message<byte[], byte[]> { Key = TestKey, Value = TestValue });
                Assert.True(drTask.IsCompleted);
                Assert.Equal(Offset.Invalid, drTask.Result.Offset);
                Assert.Equal(0, (int)drTask.Result.Partition);
                Assert.Equal(singlePartitionTopic, drTask.Result.Topic);
                Assert.Equal(TestKey, drTask.Result.Message.Key);
                Assert.Equal(TestValue, drTask.Result.Message.Value);

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            Assert.Equal(0, count);

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_DisableDeliveryReports");
        }
    }
}
