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
    public static partial class Tests
    {
        /// <summary>
        ///     Simple test of both Consumer.Assign overloads.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AssignOverloads(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start AssignOverloads");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var testString = "hello world";
            var testString2 = "hello world 2";

            DeliveryResult dr;
            using (var producer = new Producer(producerConfig))
            {
                dr = producer.ProduceAsync(singlePartitionTopic, new Message { Value = Serializers.UTF8(testString) }).Result;
                var dr2 = producer.ProduceAsync(singlePartitionTopic, new Message { Value = Serializers.UTF8(testString2) }).Result;
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new Consumer(consumerConfig))
            {
                // Explicitly specify partition offset.
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset) });
                var cr = consumer.Consume<Null, string>(TimeSpan.FromSeconds(10));
                Assert.Equal(cr.Value, testString);

                // Determine offset to consume from automatically.
                consumer.Assign(new List<TopicPartition>() { dr.TopicPartition });
                cr = consumer.Consume<Null, string>(TimeSpan.FromSeconds(10));
                Assert.NotNull(cr.Message);
                Assert.Equal(cr.Message.Value, testString2);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AssignOverloads");
        }

    }
}
