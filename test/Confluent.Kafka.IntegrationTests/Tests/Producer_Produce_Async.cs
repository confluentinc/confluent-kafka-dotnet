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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test that use of async serializers with
    ///     Produce is not possible.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Produce_Async(string bootstrapServers)
        {
            LogToFile("start Producer_Produce_Async");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            using (var testTopic = new TemporaryTopic(bootstrapServers, 1))
            using (var producer = new ProducerBuilder<Null, string>(producerConfig)
                .SetValueSerializer(new SimpleAsyncSerializer())
                .Build())
            using (var dProducer = new DependentProducerBuilder<string, Null>(producer.Handle)
                .SetKeySerializer(new SimpleAsyncSerializer())
                .Build())
            {
                Assert.Throws<ProduceException<Null, string>>(
                    () => producer.Produce(testTopic.Name, new Message<Null, string> { Value = "test" }));

                Assert.Throws<ProduceException<Null, string>>(
                    () => producer.Produce(testTopic.Name, new Message<Null, string> { Value = "test" }, dr => { Assert.True(false); }));

                Assert.Throws<ProduceException<string, Null>>(
                    () => dProducer.Produce(testTopic.Name, new Message<string, Null> { Key = "test" }));

                Assert.Throws<ProduceException<string, Null>>(
                    () => dProducer.Produce(testTopic.Name, new Message<string, Null> { Key = "test" }, dr => { Assert.True(false); }));
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_Produce_Async");
        }
    }
}
