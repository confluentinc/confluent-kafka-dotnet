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

using System;
using System.Collections.Generic;
using Xunit;
using Newtonsoft.Json;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic test that metadata request works.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void Metadata(string bootstrapServers, string topic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            using (var producer = new Producer(producerConfig))
            {
                var metadata = producer.GetMetadata(true, null);
                Assert.NotNull(metadata.Brokers);
                Assert.True(metadata.Brokers.Count > 0);

                var metadataAsJson = metadata.ToString();
                var deserialized = JsonConvert.DeserializeObject<Metadata>(metadataAsJson);

                Assert.Equal(metadata.OriginatingBrokerId, deserialized.OriginatingBrokerId);
                Assert.Equal(metadata.OriginatingBrokerName, deserialized.OriginatingBrokerName);
                Assert.Equal(metadata.Topics.Count, deserialized.Topics.Count);
                for (int i=0; i<metadata.Topics.Count; ++i)
                {
                    Assert.Equal(metadata.Topics[i].Error, deserialized.Topics[i].Error);
                    Assert.Equal(metadata.Topics[i].Topic, deserialized.Topics[i].Topic);
                    for (int j=0; j<metadata.Topics[i].Partitions.Count; ++j)
                    {
                        Assert.Equal(metadata.Topics[i].Partitions[j].Error, deserialized.Topics[i].Partitions[j].Error);
                        Assert.Equal(metadata.Topics[i].Partitions[j].Leader, deserialized.Topics[i].Partitions[j].Leader);
                        Assert.Equal(metadata.Topics[i].Partitions[j].PartitionId, deserialized.Topics[i].Partitions[j].PartitionId);
                        for (int k=0; k<metadata.Topics[i].Partitions[j].Replicas.Length; ++k)
                        {
                            Assert.Equal(metadata.Topics[i].Partitions[j].Replicas[k], deserialized.Topics[i].Partitions[j].Replicas[k]);
                        }
                        for (int k=0; k<metadata.Topics[i].Partitions[j].InSyncReplicas.Length; ++k)
                        {
                            Assert.Equal(metadata.Topics[i].Partitions[j].InSyncReplicas[k], deserialized.Topics[i].Partitions[j].InSyncReplicas[k]);
                        }
                    }
                }

                Assert.Equal(metadata.Brokers.Count, deserialized.Brokers.Count);
                for (int i=0; i<metadata.Brokers.Count; ++i)
                {
                    Assert.Equal(metadata.Brokers[i].BrokerId, deserialized.Brokers[i].BrokerId);
                    Assert.Equal(metadata.Brokers[i].Host, deserialized.Brokers[i].Host);
                    Assert.Equal(metadata.Brokers[i].Port, deserialized.Brokers[i].Port);
                }
            }
        }
    }
}
