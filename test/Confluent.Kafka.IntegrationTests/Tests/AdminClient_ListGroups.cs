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

using System;
using Xunit;
using Confluent.Kafka.TestsCommon;
using System.Threading;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_ListGroups(string bootstrapServers)
        {
            if (!TestConsumerGroupProtocol.IsClassic())
            {
                LogToFile("ListGroup not working with KIP-848 " +
                          "at the moment");
                return;
            }

            LogToFile("start AdminClient_ListGroups");

            var groupId = Guid.NewGuid().ToString();
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers })
                .Build();
            IConsumer<Ignore, Ignore>[] consumers = new IConsumer<Ignore, Ignore>[10];
            var i = 0;
            try
            {
                for (; i < 10; i++)
                {
                    consumers[i] = new TestConsumerBuilder<Ignore, Ignore>(
                        consumerConfig).Build();
                    consumers[i].Subscribe(topic.Name);
                    // Ensure it joins the group.
                    Thread.Sleep(2000);
                    var info = admin.ListGroup(groupId,
                        TimeSpan.FromSeconds(2));
                    Assert.NotNull(info);
                    Assert.Equal(i + 1, info.Members.Count);
                }
            }
            finally
            {
                for (var j = 0; j < i; j++)
                {
                    if (consumers[j] != null)
                        consumers[j].Close();
                }
            }
            
            LogToFile("end   AdminClient_ListGroups");
        }
    }
}