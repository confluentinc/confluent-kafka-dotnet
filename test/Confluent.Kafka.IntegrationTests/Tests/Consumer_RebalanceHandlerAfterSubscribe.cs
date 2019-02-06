// Copyright 2019 Confluent Inc.
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
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that SetRebalanceHandler can't be set after Subscribe has been called.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Consumer_RebalanceHandlerAfterSubscribe(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start Consumer_RebalanceHandlerAfterSubscribe");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.Subscribe(singlePartitionTopic);
                Assert.Throws<InvalidOperationException>(() => consumer.SetRebalanceHandler((_, e) => {}));
                consumer.Close();
            }

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                consumer.SetRebalanceHandler((_, e) => {});
                Assert.Throws<InvalidOperationException>(() => consumer.SetRebalanceHandler((_, e) => {}));
                consumer.Close();
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Consumer_RebalanceHandlerAfterSubscribe");
        }

    }
}
