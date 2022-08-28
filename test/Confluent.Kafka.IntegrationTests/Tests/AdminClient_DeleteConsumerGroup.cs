// Copyright 2022 Confluent Inc.
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

using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests 
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DeleteConsumerGroup(string bootstrapServers)
        {
            LogToFile("start AdminClient_DeleteConsumerGroup");

            var groupId = Guid.NewGuid().ToString();
            var groupId2 = Guid.NewGuid().ToString();
            var groupId3 = Guid.NewGuid().ToString();
            using var topic = new TemporaryTopic(bootstrapServers, 1);
            Util.ProduceNullStringMessages(bootstrapServers, topic.Name, 1, 1);

            using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // test single delete group
                CreateConsumer(bootstrapServers, groupId, topic.Name);

                admin.DeleteGroupsAsync(new List<string> { groupId }, new DeleteGroupsOptions()).Wait();

                var groups = admin.ListGroups(TimeSpan.FromSeconds(5));
                Assert.DoesNotContain(groups, (group) => group.Group == groupId);

                // test
                //  - delete two groups, one that doesn't exist.
                CreateConsumer(bootstrapServers, groupId2, topic.Name);

                try
                {
                    admin.DeleteGroupsAsync(new List<string> {groupId2, groupId3}, new DeleteGroupsOptions()).Wait();
                    Assert.True(false); // expecting exception.
                }
                catch (AggregateException ex)
                {
                    var dge = (DeleteGroupsException)ex.InnerException;
                    Assert.Equal(2, dge.Results.Count);
                    Assert.Single(dge.Results.Where(r => r.Error.IsError));
                    Assert.Single(dge.Results.Where(r => !r.Error.IsError));
                    Assert.Equal(groupId2, dge.Results.Where(r => !r.Error.IsError).First().Group);
                    Assert.Equal(groupId3, dge.Results.Where(r => r.Error.IsError).First().Group);
                }
            };

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DeleteConsumerGroup");
        }

        private static void CreateConsumer(string bootstrapServers, string groupId, string topic)
        {
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();

            consumer.Subscribe(topic);

            var cr = consumer.Consume(TimeSpan.FromSeconds(10));

            consumer.Commit(cr);

            consumer.Close();
        }
    }
}
