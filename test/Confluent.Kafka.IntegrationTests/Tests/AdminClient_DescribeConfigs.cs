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
        ///     Test functionality of AdminClient.DescribeConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async static void AdminClient_DescribeConfigs(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var configResource = new ConfigResource { Name = "0", ResourceType = ConfigType.Broker };

                // try
                // {
                //     var result = adminClient.DescribeConfigsConcurrent(new List<ConfigResource> { configResource });
                //     foreach (var r in result)
                //     {
                //         // variation 1: the await could be omitted to allow tasks to complete out-of-order on different threads.
                //         // variation 2: the .ContinueWith call could be appended with .Wait() for
                //         await r.ContinueWith(completedTask => 
                //         {
                //             if (completedTask.IsFaulted)
                //             {
                //                 Console.WriteLine($"problem: {completedTask.Exception.ToString()}");
                //             }
                //             else 
                //             {
                //                 Console.WriteLine($"do the thing: {completedTask.Result}");
                //             }
                //         });
                //     }
                // }
                // catch (Exception ex)
                // {
                    
                // }

                var d = new Dictionary<string, string>();
                
                try
                {
                    var results = await adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource });

                    foreach (var r in results)
                    {
                        Console.WriteLine($"configs: {r.Entries["sdf"]}");
                    }
                }
                catch (DescribeConfigsException ex)
                {
                    var inError = ex.Results.Where(r => r.Error.HasError);
                }

    /*
                Assert.Single(result);
                Assert.Equal(ErrorCode.NoError, result[configResource].Error.Code);
                Assert.True(result[configResource].Entries.Count > 50);
                Assert.Single(result[configResource].Entries.Where(e => e.Name == "advertised.listeners"));
                Assert.Single(result[configResource].Entries.Where(e => e.Name == "num.network.threads"));
    */
            }
        }

/*
        /// <summary>
        ///     Test functionality of AdminClient.DescribeConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void AdminClient_DescribeConfigs_II(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            using (var adminClient = new AdminClient(new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } }))
            {
                var cr = new ConfigResource { Name = "0", ResourceType = ConfigType.Broker };
                var tcr1 = new ConfigResource { Name = "topic-doesnt-exist", ResourceType = ConfigType.Topic };
                var tcr2 = new ConfigResource { Name = singlePartitionTopic, ResourceType = ConfigType.Topic };

                var result = adminClient.DescribeConfigsAsync(new List<ConfigResource> { cr, tcr1, tcr2 }).Result;


                var result2 = await adminClient.DescribeConfigsAsync(new List<ConfigResource> { cr });
                
                Assert.Equal(3, result.Count);
            }
        }


        class AdminClient1 
        {
            public async Task<Dictionary<ConfigResource, ConfigResult>> DescribeConfigsAsync(List<ConfigResource> crs)
                => await Task.FromResult(new Dictionary<ConfigResource, ConfigResult>());
        }

        class AdminClient2
        {
            public Dictionary<ConfigResource, Task<Config>> DescribeConfigsAsync(List<ConfigResource> crs)
                => new Dictionary<ConfigResource, Task<Config>>();
        }

        class AdminClient3
        {
            public async Task<Dictionary<ConfigResource, Task<Config>>> DescribeConfigsAsync(List<ConfigResource> crs)
                => await Task.FromResult(new Dictionary<ConfigResource, Task<Config>>());
        }

        class ConfigResult2
        {
            public ConfigResource ConfigResource { get; set; }
            public Er
        }

        class AdminClient4
        {
            public async Task<List<ConfigResult2>> DescribeConfigsAsync(List<ConfigResource> crs)
                => await Task.FromResult(new List<ConfigResult2>());
        }

        public static async void Test1()
        {
            var ac1 = new AdminClient1();
            var ac2 = new AdminClient2();
            var ac3 = new AdminClient3();
            var ac4 = new AdminClient4();

            // will throw on request level errors. 
            var result1 = await ac1.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource { Name = "0", ResourceType = ConfigType.Broker } });
            result1.ToList().ForEach(r => Console.WriteLine(r.Key + " " + r.Value));
            
            // This option is easily discounted, because you can't await it (awaitable methods must return a Task).
            var result2 = ac2.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource { Name = "0", ResourceType = ConfigType.Broker } });

            var result3 = await ac3.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource { Name = "0", ResourceType = ConfigType.Broker } });            

            var result4 = await ac4.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource { Name = "0", ResourceType = ConfigType.Broker } });

            result4.ForEach(r => Console.WriteLine(r.ConfigResource));

            foreach (var r in result1)
            {

            }
        }
*/

    }
}
