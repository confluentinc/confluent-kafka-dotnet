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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test of disabling marshaling of message headers.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_DisableTimestamps(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_DisableTimestamps");

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "acks", "all" },
                { "bootstrap.servers", bootstrapServers },
                { "dotnet.consumer.consume.result.fields", "topic,headers" },
                { "error_cb", (Action<ErrorEvent>)(e => Assert.True(false, e.Error.Reason)) }
            };

            var producerConfig = new Dictionary<string, object> { {"bootstrap.servers", bootstrapServers}};

            DeliveryReport<Null, string> dr;
            using (var producer = new Producer<Null, string>(producerConfig))
            {
                dr = producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<Null, string>
                    {
                        Value = "my-value", 
                        Headers = new Headers() { new Header("my-header", new byte[] { 42 }) }
                    }
                ).Result;
            }

            using (var consumer = new Consumer<Null, string>(consumerConfig))
            {
                consumer.Assign(new TopicPartitionOffset[] { new TopicPartitionOffset(singlePartitionTopic, 0, dr.Offset) });

                var record = consumer.Consume(TimeSpan.FromSeconds(30));
                Assert.NotNull(record.Message);
                Assert.NotNull(record.Message.Headers);
                Assert.Equal(TimestampType.NotAvailable, record.Timestamp.Type);
                Assert.Equal(0, record.Timestamp.UnixTimestampMs);
            }
            
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_DisableTimestamps");
        }

    }
}
