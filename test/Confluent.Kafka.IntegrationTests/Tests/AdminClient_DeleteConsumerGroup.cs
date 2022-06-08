﻿using Confluent.Kafka.Admin;
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

            var topic = Guid.NewGuid().ToString();

            var groudId1 = Guid.NewGuid().ToString();
            var groudId2 = Guid.NewGuid().ToString();
            var groudId3 = Guid.NewGuid().ToString();

            using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                CreateSomeDummyTopic(admin, topic);

                // test single delete group
                CreateConsumer(bootstrapServers, groudId1, topic);

                admin.DeleteGroupAsync(new List<string> { groudId1 }, new DeleteGroupOptions()).Wait();

                var groups = admin.ListGroups(TimeSpan.FromSeconds(5));
                Assert.DoesNotContain(groups, (group) => group.Group == groudId1);

                // test
                //  - delete two groups, one that doesn't exist.
                CreateConsumer(bootstrapServers, groudId2, topic);

                try
                {
                    admin.DeleteGroupAsync(new List<string> { groudId2, groudId3 }, new DeleteGroupOptions()).Wait();
                }
                catch (AggregateException ex)
                {
                    var dge = (DeleteGroupsException)ex.InnerException;
                    Assert.Equal(2, dge.Results.Count);
                    Assert.Single(dge.Results.Where(r => r.Error.IsError));
                    Assert.Single(dge.Results.Where(r => !r.Error.IsError));
                    Assert.Equal(groudId2, dge.Results.Where(r => !r.Error.IsError).First().Group);
                    Assert.Equal(groudId3, dge.Results.Where(r => r.Error.IsError).First().Group);
                }
            };

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DeleteConsumerGroup");
        }

        private static void CreateSomeDummyTopic(IAdminClient admin, string topic)
        {
            admin.CreateTopicsAsync(
                    new TopicSpecification[]
                    {
                        new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 },
                    }).Wait();

            Thread.Sleep(TimeSpan.FromSeconds(2));
        }

        private static void CreateConsumer(string bootstrapServers, string groupId, string topic)
        {
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId
            }).Build();

            consumer.Subscribe(topic);

            Thread.Sleep(TimeSpan.FromSeconds(2));

            consumer.Close();

            Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }
}
