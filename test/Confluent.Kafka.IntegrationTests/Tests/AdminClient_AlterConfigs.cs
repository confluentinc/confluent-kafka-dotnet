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
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.AlterConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_AlterConfigs(string bootstrapServers)
        {
            LogToFile("start AdminClient_AlterConfigs");
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // 1. create a new topic to play with.
                string topicName = Guid.NewGuid().ToString();
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                Thread.Sleep(TimeSpan.FromSeconds(1)); // without this, sometimes describe topic throws unknown topic/partition error.

                // 2. do an invalid alter configs call to change it.
                var configResource = new ConfigResource { Name = topicName, Type = ResourceType.Topic };
                var toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    {
                        configResource,
                        new List<ConfigEntry> {
                            new ConfigEntry { Name = "flush.ms", Value="10001" },
                            new ConfigEntry { Name = "ubute.invalid.config", Value="42" }
                        }
                    }
                };
                try
                {
                    adminClient.AlterConfigsAsync(toUpdate).Wait();
                    Assert.True(false);
                }
                catch (Exception e)
                {
                    Assert.True(e.InnerException.GetType() == typeof(AlterConfigsException));
                    var ace = (AlterConfigsException)e.InnerException;
                    Assert.Single(ace.Results);
                    Assert.Contains("Unknown", ace.Results[0].Error.Reason);
                }

                // 3. test that in the failed alter configs call for the specified config resource, the 
                // config that was specified correctly wasn't updated.
                List<DescribeConfigsResult> describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.NotEqual("10001", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 4. do a valid call, and check that the alteration did correctly happen.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="10011" } } } 
                };
                adminClient.AlterConfigsAsync(toUpdate);
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10011", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 4. test ValidateOnly = true does not update config entry.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="20002" } } } 
                };
                adminClient.AlterConfigsAsync(toUpdate, new AlterConfigsOptions { ValidateOnly = true }).Wait();
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10011", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 5. test updating broker resource. 
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { 
                        new ConfigResource { Name = "0", Type = ResourceType.Broker },
                        new List<ConfigEntry> { new ConfigEntry { Name="num.network.threads", Value="6" } }
                    }
                };
                adminClient.AlterConfigsAsync(toUpdate).Wait();
                
                // 6. test updating more than one resource.
                string topicName2 = Guid.NewGuid().ToString();
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                Thread.Sleep(TimeSpan.FromSeconds(1)); // without this, sometimes describe topic throws unknown topic/partition error.

                var configResource2 = new ConfigResource { Name = topicName2, Type = ResourceType.Topic };
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="222" } } },
                    { configResource2, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="333" } } }
                };
                adminClient.AlterConfigsAsync(toUpdate).Wait();
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource, configResource2 }).Result;
                Assert.Equal(2, describeConfigsResult.Count);
                Assert.Equal("222", describeConfigsResult[0].Entries["flush.ms"].Value);
                Assert.Equal("333", describeConfigsResult[1].Entries["flush.ms"].Value);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_AlterConfigs");
        }
    }
}
