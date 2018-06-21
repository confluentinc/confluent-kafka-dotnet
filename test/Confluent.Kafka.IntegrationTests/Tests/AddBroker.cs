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
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Xunit;


/*
namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that produces a message then consumes it.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AddBrokers(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            // This test assumes broker v0.10.0 or higher:
            // https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility

            // This test does a broker metadata request, as it's an easy way to see 
            // if we are connected to broker. It's not really the best way to test this
            // (ideally, we would get from a working list of brokers to all brokers 
            // that have changed IP) but this will be good enough.

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", "unknown" }
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", Guid.NewGuid().ToString() },
                { "bootstrap.servers", "unknown" },
                { "session.timeout.ms", 6000 }
            };

            using (var typedProducer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                TestMetadata(
                    () => typedProducer.GetMetadata(false, null, TimeSpan.FromSeconds(3)),
                    typedProducer.AddBrokers);
            }

            using (var producer = new Producer<byte[], byte[]>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer()))
            {
                TestMetadata(
                    () => producer.GetMetadata(false, null, TimeSpan.FromSeconds(3)),
                    producer.AddBrokers);
            }

            using (var consumer = new Consumer<byte[], byte[]>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
            {
                TestMetadata(
                    () => consumer.GetMetadata(false, TimeSpan.FromSeconds(3)),
                    consumer.AddBrokers);
            }

            using (var typedConsumer = new Consumer<Null, Null>(consumerConfig, new NullDeserializer(), new NullDeserializer()))
            {
                TestMetadata(
                    () => typedConsumer.GetMetadata(false, TimeSpan.FromSeconds(3)),
                    typedConsumer.AddBrokers);
            }

            void TestMetadata(Func<Metadata> getMetadata, Func<string, int> addBrokers)
            {
                try
                {
                    var metadata = getMetadata();
                    Assert.True(false, "Broker should not be reached here");
                }
                catch (KafkaException e)
                {
                    Assert.Equal(ErrorCode.Local_Transport, e.Error.Code);
                }

                int brokersAdded = addBrokers(bootstrapServers);
                Assert.True(brokersAdded > 0, "Should have added one broker or more");

                brokersAdded = addBrokers(bootstrapServers);
                Assert.True(brokersAdded > 0, "Should have added one broker or more (duplicates considered added)");

                var newMetadata = getMetadata();
                Assert.True(newMetadata.Brokers.Count > 0);

                brokersAdded = addBrokers("");
                Assert.True(brokersAdded == 0, "Should not have added brokers");
            }
        }
    }
}
*/