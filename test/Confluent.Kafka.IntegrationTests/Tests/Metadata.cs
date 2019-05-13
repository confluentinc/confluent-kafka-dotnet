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

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Basic test that metadata request + serialization works.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Metadata(string bootstrapServers)
        {
            LogToFile("start Metadata");

            var config = new AdminClientConfig { BootstrapServers = bootstrapServers };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                Assert.NotNull(metadata.Brokers);
                Assert.True(metadata.Brokers.Count > 0);

                var metadataAsJson = metadata.ToString();
                var deserialized = (JObject)Newtonsoft.Json.JsonConvert.DeserializeObject(metadataAsJson);

                Assert.Equal(deserialized.Value<int>("OriginatingBrokerId"), metadata.OriginatingBrokerId);
                Assert.Equal(deserialized.Value<string>("OriginatingBrokerName"), metadata.OriginatingBrokerName);
                var topics = new List<JToken>(deserialized["Topics"].Children());
                Assert.Equal(metadata.Topics.Count, topics.Count);
                for (int i=0; i<metadata.Topics.Count; ++i)
                {
                    Assert.Equal(topics[i].Value<string>("Error"), metadata.Topics[i].Error.Code.ToString());
                    Assert.Equal(topics[i].Value<string>("Topic"), metadata.Topics[i].Topic);
                    var partitions = new List<JToken>(topics[i]["Partitions"].Children());
                    Assert.Equal(partitions.Count, metadata.Topics[i].Partitions.Count);
                    for (int j=0; j<metadata.Topics[i].Partitions.Count; ++j)
                    {
                        Assert.Equal(partitions[j].Value<string>("Error"), metadata.Topics[i].Partitions[j].Error.Code.ToString());
                        Assert.Equal(partitions[j].Value<int>("Leader"), metadata.Topics[i].Partitions[j].Leader);
                        Assert.Equal(partitions[j].Value<int>("PartitionId"), metadata.Topics[i].Partitions[j].PartitionId);
                        var replicas = new List<JToken>(partitions[j]["Replicas"].Children());
                        Assert.Equal(replicas.Count, metadata.Topics[i].Partitions[j].Replicas.Length);
                        for (int k=0; k<metadata.Topics[i].Partitions[j].Replicas.Length; ++k)
                        {
                            Assert.Equal(replicas[k].Value<int>(), metadata.Topics[i].Partitions[j].Replicas[k]);
                        }
                        var inSyncReplicas = new List<JToken>(partitions[j]["InSyncReplicas"].Children());
                        Assert.Equal(inSyncReplicas.Count, metadata.Topics[i].Partitions[j].InSyncReplicas.Length);
                        for (int k=0; k<metadata.Topics[i].Partitions[j].InSyncReplicas.Length; ++k)
                        {
                            Assert.Equal(inSyncReplicas[k].Value<int>(), metadata.Topics[i].Partitions[j].InSyncReplicas[k]);
                        }
                    }
                }

                var brokers = new List<JToken>(deserialized["Brokers"].Children());
                Assert.Equal(metadata.Brokers.Count, brokers.Count);
                for (int i=0; i<metadata.Brokers.Count; ++i)
                {
                    Assert.Equal(metadata.Brokers[i].BrokerId, brokers[i].Value<int>("BrokerId"));
                    Assert.Equal(metadata.Brokers[i].Host, brokers[i].Value<string>("Host"));
                    Assert.Equal(metadata.Brokers[i].Port, brokers[i].Value<int>("Port"));
                }
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Metadata");
        }
    }
}
