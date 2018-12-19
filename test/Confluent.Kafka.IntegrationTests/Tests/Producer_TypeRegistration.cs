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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test of disabling marshaling of message headers.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Producer_TypeRegistration(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Producer_TypeRegistration");

            DeliveryResult<Null, string> stringDeliveryReport;
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            using (var producer = new Producer(producerConfig))
            {
                // check for ArgumentException in the case the type isn't known.
                Assert.Throws<ArgumentException>(() =>
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<Decimal, string> { Key = 1.0M, Value = "won't work" }).Wait());

                Assert.Throws<ArgumentException>(() =>
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<string, Decimal> { Key = "won't work", Value = 2.0M }).Wait());

                // check that unregister works.
                producer.ProduceAsync(singlePartitionTopic, new Message<int, int> { Key = 42, Value = 43 });
                producer.UnregisterSerializer<int>();
                Assert.Throws<ArgumentException>(() =>
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<int, int> { Key = 42, Value = 43 }).Wait());

                // check register override works (part a)
                producer.RegisterSerializer<string>(Encoding.UTF32.GetBytes);
                stringDeliveryReport = producer.ProduceAsync<Null, string>(singlePartitionTopic, new Message<Null, string> { Value = "test string" }).Result;
            }

            // check register override works (part b)
            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(stringDeliveryReport.TopicPartitionOffset);
                var cr = consumer.Consume<Null, byte[]>();
                var str = Encoding.UTF32.GetString(cr.Value);
                Assert.Equal("test string", str);
                Assert.NotEqual(cr.Value.Length, Encoding.UTF8.GetBytes("test string").Length);
            }

            LogToFile("end   Producer_TypeRegistration");
        }
    }
}