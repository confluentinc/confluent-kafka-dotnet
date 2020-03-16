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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Simple test of all Consumer.Assign overloads.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AssignOverloads(string bootstrapServers)
        {
            LogToFile("start AssignOverloads");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000,
                EnableAutoCommit = false
            };
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var testString = "hello world";
            var testString2 = "hello world 2";
            var testString3 = "hello world 3";
            var testString4 = "hello world 4";

            DeliveryResult<Null, string> dr, dr3;
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                dr = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = testString }).Result;
                producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = testString2 }).Wait();
                dr3 = producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = testString3 }).Result;
                producer.ProduceAsync(singlePartitionTopic, new Message<Null, string> { Value = testString4 }).Wait();
                producer.Flush(TimeSpan.FromSeconds(10));
            }

            using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build())
            {
                // Explicitly specify partition offset.
                consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(dr.TopicPartition, dr.Offset) });
                var cr = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Commit();
                Assert.Equal(cr.Message.Value, testString);
                
                // Determine offset to consume from automatically.
                consumer.Assign(new List<TopicPartition>() { dr.TopicPartition });
                cr = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Commit();
                Assert.NotNull(cr.Message);
                Assert.Equal(cr.Message.Value, testString2);

                // Explicitly specify partition offset.
                consumer.Assign(new TopicPartitionOffset(dr.TopicPartition, dr3.Offset));
                cr = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Commit();
                Assert.Equal(cr.Message.Value, testString3);

                // Determine offset to consume from automatically.
                consumer.Assign(dr.TopicPartition);
                cr = consumer.Consume(TimeSpan.FromSeconds(10));
                consumer.Commit();
                Assert.NotNull(cr.Message);
                Assert.Equal(cr.Message.Value, testString4);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AssignOverloads");
        }

    }
}
