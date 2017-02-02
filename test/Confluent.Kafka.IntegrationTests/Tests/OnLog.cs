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

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests that log messages are received by OnLog on all Producer and Consumer variants.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void OnLog(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "test-consumer-group" },
                { "bootstrap.servers", bootstrapServers },
                { "log_level", 7 },
                { "debug", "all" }
            };

            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "log_level", 7 },
                { "debug", "all" }
            };

            // byte array producer.
            int logCount = 0;
            using (var producer = new Producer(producerConfig))
            {
                producer.OnLog += (_, LogMessage)
                  => logCount += 1;

                producer.ProduceAsync(topic, null, (byte[])null).Wait();
                producer.Flush();
            }
            Assert.True(logCount > 0);

            // serializing producer.
            Message<Null, string> dr;
            logCount = 0;
            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                producer.OnLog += (_, LogMessage)
                  => logCount += 1;

                dr = producer.ProduceAsync(topic, null, "test value").Result;
                producer.Flush();
            }
            Assert.True(logCount > 0);

            // wrapped byte array producer.
            logCount = 0;
            using (var producer = new Producer(producerConfig))
            {
                var sProducer = producer.GetSerializingProducer<Null, string>(null, new StringSerializer(Encoding.UTF8));

                sProducer.OnLog += (_, LogMessage)
                  => logCount += 1;

                sProducer.ProduceAsync(topic, null, "test value").Wait();
                producer.Flush();
            }
            Assert.True(logCount > 0);

            // byte array consumer.
            logCount = 0;
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.OnLog += (_, LogMessage)
                  => logCount += 1;

                consumer.Poll(TimeSpan.FromMilliseconds(100));
            }
            Assert.True(logCount > 0);

            // deserializing consumer.
            logCount = 0;
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnLog += (_, LogMessage)
                  => logCount += 1;

                consumer.Poll(TimeSpan.FromMilliseconds(100));
            }
            Assert.True(logCount > 0);
        }

    }
}
