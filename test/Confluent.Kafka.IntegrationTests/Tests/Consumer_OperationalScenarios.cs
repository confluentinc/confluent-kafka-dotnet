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
using System.Collections.Generic;
using System.Text;
using Xunit;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Tests various
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_OperationalScenarios(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "simple-produce-consume" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            // VALID Scenarios

            // start then stop then start a Consumer
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Start();
                consumer.Stop();
                consumer.Start();
            }
            // no problem expected.

            // start then stop then start a deserializing Consumer
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Start();
                consumer.Stop();
                consumer.Start();
            }
            // no problem expected.


            // ERROR Scenarios

            // start a Consumer twice
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Start();
                Assert.Throws<Exception>(() => consumer.Start());
            }

            // start a deserializing Consumer twice
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Start();
                Assert.Throws<Exception>(() => consumer.Start());
            }

            // stop an unstarted Consumer.
            using (var consumer = new Consumer(consumerConfig))
            {
                Assert.Throws<Exception>(() => consumer.Stop());
            }

            // stop an unstarted deserializing Consumer.
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                Assert.Throws<Exception>(() => consumer.Stop());
            }

            // stop a Consumer twice.
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Start();
                consumer.Stop();
                Assert.Throws<Exception>(() => consumer.Stop());
            }

            // stop a deserializing Consumer twice
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Start();
                consumer.Stop();
                Assert.Throws<Exception>(() => consumer.Stop());
            }

            // start a Consumer then attempt to Poll.
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Start();
                Assert.Throws<Exception>(() => consumer.Poll(100));
            }

            // start a deserializing Consumer then attempt to Poll.
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Start();
                Assert.Throws<Exception>(() => consumer.Poll(100));
            }

            // start a Consumer then attempt to call Consume.
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Start();
                Message msg;
                Assert.Throws<Exception>(() => consumer.Consume(out msg, 100));
            }

            // start a deserializing Consumer then attempt to call Consume.
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Start();
                Message<Null, string> msg;
                Assert.Throws<Exception>(() => consumer.Consume(out msg, 100));
            }

        }


    }
}
