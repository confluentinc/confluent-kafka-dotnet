// Copyright 2020 Confluent Inc.
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
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that setting client.software.name + client.software.version
        ///     in producer and consumer config does not result in an exception.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void ClientNameVersion(string bootstrapServers)
        {
            LogToFile("start ClientNameVersion");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };
            producerConfig.Set("client.software.name", "test");
            producerConfig.Set("client.software.version", "1.0");

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };
            consumerConfig.Set("client.software.name", "test");
            consumerConfig.Set("client.software.version", "1.0");


            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            { }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   ClientNameVersion");
        }
    }
}