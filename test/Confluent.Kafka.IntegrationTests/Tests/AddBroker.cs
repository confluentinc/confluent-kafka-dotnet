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
    public partial class Tests
    {
        /// <summary>
        ///     Test the AddBroker method works.
        /// </summary>
        /// <remarks>
        ///     Assumes broker v0.10.0 or higher:
        ///     https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility
        ///     A metadata request is used to check if there is a broker connection.
        /// </remarks>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AddBrokers(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:65533" };

            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                try
                {
                    var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
                    Assert.True(false, "Broker should not be reached here");
                }
                catch (KafkaException e)
                {
                    Assert.Equal(ErrorCode.Local_Transport, e.Error.Code);
                }

                // test is > 0 note == 1 since bootstrapServers could include more than one broker.
                int brokersAdded = adminClient.AddBrokers(bootstrapServers);
                Assert.True(brokersAdded > 0, "Should have added one broker or more");

                brokersAdded = adminClient.AddBrokers(bootstrapServers);
                Assert.True(brokersAdded > 0, "Should have added one broker or more (duplicates considered added)");

                var newMetadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
                Assert.True(newMetadata.Brokers.Count >= 1);

                brokersAdded = adminClient.AddBrokers("");
                Assert.True(brokersAdded == 0, "Should not have added brokers");

                newMetadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
                Assert.True(newMetadata.Brokers.Count > 0);
            }
        }
    }
}
