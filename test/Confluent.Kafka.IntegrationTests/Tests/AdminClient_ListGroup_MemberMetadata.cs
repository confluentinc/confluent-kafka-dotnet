// Copyright 2026 Confluent Inc.
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
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     ListGroup returns each member's metadata and assignment bytes.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_ListGroup_MemberMetadata(string bootstrapServers)
        {
            if (!TestConsumerGroupProtocol.IsClassic())
            {
                LogToFile("ListGroup is deprecated and won't " +
                          "work with KIP-848");
                return;
            }

            LogToFile("start AdminClient_ListGroup_MemberMetadata");

            var groupId = Guid.NewGuid().ToString();
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                EnableAutoCommit = false
            };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers })
                .Build();
            using (var consumer = new TestConsumerBuilder<Ignore, Ignore>(consumerConfig).Build())
            {
                consumer.Subscribe(topic.Name);
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (consumer.Assignment.Count == 0 && DateTime.UtcNow < deadline)
                {
                    consumer.Consume(TimeSpan.FromMilliseconds(500));
                }
                Assert.NotEmpty(consumer.Assignment);

                var info = admin.ListGroup(groupId, TimeSpan.FromSeconds(10));
                Assert.NotNull(info);
                var member = Assert.Single(info.Members);
                // Both are binary, but each contains the subscribed topic's name.
                Assert.Contains(topic.Name, Encoding.ASCII.GetString(member.MemberMetadata));
                Assert.Contains(topic.Name, Encoding.ASCII.GetString(member.MemberAssignment));

                consumer.Close();
            }

            LogToFile("end   AdminClient_ListGroup_MemberMetadata");
        }
    }
}
