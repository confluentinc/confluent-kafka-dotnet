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
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.DescribeConfigs.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DescribeConfigs(string bootstrapServers)
        {
            LogToFile("start AdminClient_DescribeConfigs");

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // broker configs
                // ---
                var configResource = new ConfigResource { Name = "0", Type = ResourceType.Broker };
                var results = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }).Result;

                Assert.Single(results);
                Assert.True(results[0].Entries.Count > 50);
                // note: unlike other parts of the api, Entries is kept as a dictionary since it's convenient for
                // the most typical use case.
                Assert.Single(results[0].Entries.Where(e => e.Key == "advertised.listeners"));
                Assert.Single(results[0].Entries.Where(e => e.Key == "num.network.threads"));

                var a = results.Select(aa => aa.Entries.Where(b => b.Value.Synonyms.Count > 0).ToList()).ToList();

                // topic configs, more than one.
                // ---
                results = adminClient.DescribeConfigsAsync(new List<ConfigResource> { 
                    new ConfigResource { Name = singlePartitionTopic, Type = ResourceType.Topic },
                    new ConfigResource { Name = partitionedTopic, Type = ResourceType.Topic }
                }).Result;

                Assert.Equal(2, results.Count);
                Assert.True(results[0].Entries.Count > 20);
                Assert.True(results[1].Entries.Count > 20);
                Assert.Single(results[0].Entries.Where(e => e.Key == "compression.type"));
                Assert.Single(results[0].Entries.Where(e => e.Key == "flush.ms"));

                // options are specified.
                // ---
                results = adminClient.DescribeConfigsAsync(new List<ConfigResource> { configResource }, new DescribeConfigsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).Result;
                Assert.Single(results);
                Assert.True(results[0].Entries.Count > 20);

                // empty config resource
                // --- 
                try
                {
                    results = adminClient.DescribeConfigsAsync(new List<ConfigResource> { new ConfigResource() }).Result;
                    Assert.True(false);
                }
                catch (ArgumentException)
                {
                    // expected.
                }

                // invalid config resource
                // ---
                try
                {
                    results = adminClient.DescribeConfigsAsync(
                        new List<ConfigResource> 
                        {
                            new ConfigResource { Name="invalid.name.for.resource", Type = ResourceType.Broker }
                        }
                    ).Result;
                    Assert.True(false);
                }
                catch (AggregateException ex)
                {
                    Assert.True(ex.InnerException.GetType() == typeof(KafkaException));
                    var ace = (KafkaException)ex.InnerException;
                    Assert.Contains("Expected an int32", ace.Message);
                }

                // invalid topic.
                // ---
                //
                // TODO: this creates the topic, then describes what it just created. what we want? does java explicitly not do this?
                // 
                // results = adminClient.DescribeConfigsAsync(new List<ConfigResource> {
                //     new ConfigResource { Name = "my-nonsense-topic", ResourceType = ConfigType.Topic }
                // }).Result;
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DescribeConfigs");
        }

    }
}
