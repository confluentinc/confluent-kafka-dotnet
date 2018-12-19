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
        public static void Consumer_TypeRegistration(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_TypeRegistration");

            var messages = ProduceMessages(bootstrapServers, singlePartitionTopic, 0, 10);

            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };
            using (var consumer = new Consumer(consumerConfig))
            {
                consumer.Assign(messages[0].TopicPartitionOffset);
                
                // check that unregistered type results in argument exception.
                Assert.Throws<ArgumentException>(() => consumer.Consume<Ignore, Decimal>());
                Assert.Throws<ArgumentException>(() => consumer.Consume<Decimal, Ignore>());

                // check that Ignore/Ignore consume progresses offset by 1.
                var cr1 = consumer.Consume<Ignore, Ignore>();
                Assert.Null(cr1.Key);
                Assert.Null(cr1.Value);
                var cr2 = consumer.Consume<string, string>();
                Assert.Equal(messages[1].Offset, cr2.Offset);

                // check unregister works
                consumer.UnregisterDeserilizer<string>();
                Assert.Throws<ArgumentException>(() => consumer.Consume<string, Ignore>());
                Assert.Throws<ArgumentException>(() => consumer.Consume<Ignore, string>());
                Assert.Throws<ArgumentException>(() => consumer.Consume<string, string>());

                // check that re-registering works
                consumer.RegisterDeserializer<string>(Deserializers.UTF8);
                var cr3 = consumer.Consume<string, string>();
                // and this doesn't throw:
                consumer.RegisterDeserializer<string>(Deserializers.UTF8);
            }

            LogToFile("end   Consumer_TypeRegistration");
        }
    }
}
