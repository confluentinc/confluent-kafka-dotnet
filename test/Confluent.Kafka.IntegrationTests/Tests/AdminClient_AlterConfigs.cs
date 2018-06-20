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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Serialization;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.AlterConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AdminClient_AlterConfigs(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                // 1. create a new topic to play with.
                string topicName = Guid.NewGuid().ToString();
                var createTopicsResult = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 } }).Result;

                // 2. do an invalid alter configs call to change it.
                var configResource = new ConfigResource { Name = topicName, ResourceType = ConfigType.Topic };
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
                    var alterConfigsResult = adminClient.AlterConfigsAsync(toUpdate).Result;
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
                var describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.NotEqual("10001", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 4. do a valid call, and check that the alteration did correctly happen.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="10001" } } } 
                };
                var rr = adminClient.AlterConfigsAsync(toUpdate).Result;
                Assert.Single(rr);
                Assert.False(rr[0].Error.IsError);
                Assert.Equal(rr[0].ConfigResource, configResource);
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10001", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 4. test ValidateOnly = true does not update config entry.
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                { 
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="20002" } } } 
                };
                rr = adminClient.AlterConfigsAsync(toUpdate, new AlterConfigsOptions { ValidateOnly = true }).Result;
                Assert.Single(rr);
                Assert.False(rr[0].Error.IsError);
                Assert.Equal(rr[0].ConfigResource, configResource);
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;
                Assert.Equal("10001", describeConfigsResult[0].Entries["flush.ms"].Value);

                // 5. test updating broker resource. 
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { 
                        new ConfigResource { Name = "0", ResourceType = ConfigType.Broker },
                        new List<ConfigEntry> { new ConfigEntry { Name="num.network.threads", Value="2" } }
                    }
                };
                try
                {
                    rr = adminClient.AlterConfigsAsync(toUpdate).Result;
                    Assert.True(false);
                }
                catch (Exception ex)
                {
                    Assert.True(ex.InnerException.GetType() == typeof(AlterConfigsException));
                    var ace = (AlterConfigsException)ex.InnerException;
                    Assert.Single(ace.Results);
                    Assert.True(ace.Results[0].Error.IsError);
                }

                // 6. test updating more than on resource.
                string topicName2 = Guid.NewGuid().ToString();
                var createTopicsResult2 = adminClient.CreateTopicsAsync(
                    new List<NewTopic> { new NewTopic { Name = topicName2, NumPartitions = 1, ReplicationFactor = 1 } }).Result;
                var configResource2 = new ConfigResource { Name = topicName2, ResourceType = ConfigType.Topic };
                toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>> 
                {
                    { configResource, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="222" } } },
                    { configResource2, new List<ConfigEntry> { new ConfigEntry { Name = "flush.ms", Value="333" } } }
                };
                rr = adminClient.AlterConfigsAsync(toUpdate).Result;
                Assert.Equal(2, rr.Count);
                Assert.False(rr[0].Error.IsError);
                Assert.False(rr[1].Error.IsError);
                Assert.Equal(rr[0].ConfigResource, configResource);
                Assert.Equal(rr[1].ConfigResource, configResource2);
                describeConfigsResult = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource, configResource2 }).Result;
                Assert.Equal(2, describeConfigsResult.Count);
                Assert.False(describeConfigsResult[0].Error.IsError);
                Assert.Equal("222", describeConfigsResult[0].Entries["flush.ms"].Value);
                Assert.Equal("333", describeConfigsResult[1].Entries["flush.ms"].Value);
            }
        }
    }
}
