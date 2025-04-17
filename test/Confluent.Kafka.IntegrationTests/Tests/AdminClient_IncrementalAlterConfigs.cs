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
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;
using Confluent.Kafka.TestsCommon;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.IncrementalAlterConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_IncrementalAlterConfigs(string bootstrapServers)
        {
            LogToFile("start AdminClient_IncrementalAlterConfigs");

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // 1. create new topics to play with.
                string topicName = Guid.NewGuid().ToString(), topicName2 = Guid.NewGuid().ToString();
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 }, new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                Thread.Sleep(TimeSpan.FromSeconds(1)); // without this, sometimes describe topic throws unknown topic/partition error.

                // 2. do an invalid alter configs call to change it.
                // flush.ms is not a list type config value.
                var configResource = new ConfigResource { Name = topicName, Type = ResourceType.Topic };
                var toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    {
                        configResource,
                        new List<ConfigEntry> {
                            new ConfigEntry { Name = "cleanup.policy", Value = "compact", IncrementalOperation = AlterConfigOpType.Append },
                            new ConfigEntry { Name = "flush.ms", Value = "10001", IncrementalOperation = AlterConfigOpType.Append }
                        }
                    }
                };
                try
                {
                    adminClient.IncrementalAlterConfigsAsync(toUpdate).Wait();
                    Assert.True(false);
                }
                catch (Exception e)
                {
                    Assert.True(e.InnerException.GetType() == typeof(IncrementalAlterConfigsException));
                    var ace = (IncrementalAlterConfigsException)e.InnerException;
                    Assert.Single(ace.Results);
                    Assert.True(ace.Results[0].Error.Reason.Contains("not allowed") ||
                        ace.Results[0].Error.Reason.Contains("Can't APPEND"));
                }

                // 3. test that in the failed alter configs call for the specified config resource, the 
                // config that was specified correctly isn't updated.
                List<DescribeConfigsResult> describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.NotEqual("delete,compact", describeConfigsResult[0].Entries["cleanup.policy"].Value);

                // 4. do a valid call, and check that the alteration did correctly happen.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { 
                        configResource,
                        new List<ConfigEntry> {
                            new ConfigEntry { Name = "flush.ms", Value = "10001", IncrementalOperation = AlterConfigOpType.Set  },
                            new ConfigEntry { Name = "cleanup.policy", Value = "compact", IncrementalOperation = AlterConfigOpType.Append } 
                        } 
                    } 
                };
                adminClient.IncrementalAlterConfigsAsync(toUpdate);
                Thread.Sleep(TimeSpan.FromMilliseconds(200));
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10001", describeConfigsResult[0].Entries["flush.ms"].Value);
                Assert.Equal("delete,compact", describeConfigsResult[0].Entries["cleanup.policy"].Value);

                // 5. test ValidateOnly = true does not update config entry.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value = "20002" , IncrementalOperation = AlterConfigOpType.Set } } } 
                };
                adminClient.IncrementalAlterConfigsAsync(toUpdate, new IncrementalAlterConfigsOptions { ValidateOnly = true }).Wait();
                Thread.Sleep(TimeSpan.FromMilliseconds(200));
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10001", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 6. test updating broker resource. 
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { 
                        new ConfigResource { Name = "0", Type = ResourceType.Broker },
                        new List<ConfigEntry> { new ConfigEntry { Name = "num.network.threads", Value = "6" , IncrementalOperation = AlterConfigOpType.Set } }
                    }
                };
                adminClient.IncrementalAlterConfigsAsync(toUpdate).Wait();
                
                // 7. test updating more than one resource.
                var configResource2 = new ConfigResource { Name = topicName2, Type = ResourceType.Topic };
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value = "222" , IncrementalOperation = AlterConfigOpType.Set } } },
                    { configResource2, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value = "333" , IncrementalOperation = AlterConfigOpType.Set } } }
                };
                adminClient.IncrementalAlterConfigsAsync(toUpdate).Wait();
                Thread.Sleep(TimeSpan.FromMilliseconds(200));
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource, configResource2 }).Result;
                Assert.Equal(2, describeConfigsResult.Count);
                Assert.Equal("222", describeConfigsResult[0].Entries["flush.ms"].Value);
                Assert.Equal("333", describeConfigsResult[1].Entries["flush.ms"].Value);

                // TODO: enable this test for the classic run too, when
                // Confluent Platform test cluster is upgraded to 8.0.0.
                if(!TestConsumerGroupProtocol.IsClassic()) {
                    // 8. test updating ResourceType.Group
                    string groupName = Guid.NewGuid().ToString();
                    LogToFile($"Testing IncrementalAlterConfigs for consumer group {groupName}");
                    var groupConfigResource = new ConfigResource { Name = groupName, Type = ResourceType.Group };
                    var groupToUpdate = new Dictionary<ConfigResource, List<ConfigEntry>>
                    {
                        {
                            groupConfigResource,
                            new List<ConfigEntry> {
                                new ConfigEntry { Name = "consumer.session.timeout.ms", Value = "50000", IncrementalOperation = AlterConfigOpType.Set }
                            }
                        }
                    };
                    adminClient.IncrementalAlterConfigsAsync(groupToUpdate).Wait();
                    Thread.Sleep(TimeSpan.FromMilliseconds(200));
                    var describeGroupConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { groupConfigResource }).Result;
                    Assert.Single(describeGroupConfigsResult);
                    Assert.Equal("50000", describeGroupConfigsResult[0].Entries["consumer.session.timeout.ms"].Value);
                    LogToFile($"Successfully updated consumer.group {groupName} config");
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_IncrementalAlterConfigs");
        }
    }
}
