// Copyright 2023 Confluent Inc.
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
        /// Test that the SetSaslCredentials method doesn't crash for each of
        /// Producer, Consumer, and AdminClient.
        /// Also tests failure when username/password is null.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void SetSaslCredentials(string bootstrapServers)
        {
            LogToFile("start SetSaslCredentials");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                producer.SetSaslCredentials("username", "password");

                var errsEncountered = false;
                try
                {
                    producer.SetSaslCredentials(null, null);
                }
                catch (KafkaException ke)
                {
                    Assert.StartsWith("Username and password are required", ke.Message);
                    errsEncountered = true;
                }
                Assert.True(errsEncountered);
            }

            var consumerConfig = new ConsumerConfig {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString() };
            using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build())
            {
                consumer.SetSaslCredentials("username", "password");
                consumer.Close();
            }

            var adminClientConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                adminClient.SetSaslCredentials("username", "password");
            }

            LogToFile("end SetSaslCredentials");
        }
    }
}
