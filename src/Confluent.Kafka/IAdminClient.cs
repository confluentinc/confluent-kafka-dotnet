// Copyright 2018 Confluent Inc.
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
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines an Apache Kafka admin client.
    /// </summary>
    public interface IAdminClient : IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.ListGroups(TimeSpan)" />
        /// </summary>
        List<GroupInfo> ListGroups(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.ListGroup(string, TimeSpan)" />
        /// </summary>
        GroupInfo ListGroup(string group, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.GetWatermarkOffsets(TopicPartition)" />
        /// </summary>
        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.QueryWatermarkOffsets(TopicPartition, TimeSpan)" />
        /// </summary>
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.GetMetadata(bool, string, TimeSpan)" />
        /// </summary>
        Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.GetMetadata(bool, TimeSpan)" />
        /// </summary>
        Metadata GetMetadata(bool allTopics, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.CreatePartitionsAsync(IEnumerable{PartitionsSpecification}, CreatePartitionsOptions)" />
        /// </summary>
        Task<List<CreatePartitionsResult>> CreatePartitionsAsync(
            IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.DeleteTopicsAsync(IEnumerable{string}, DeleteTopicsOptions)" />
        /// </summary>
        Task<List<DeleteTopicResult>> DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.CreateTopicsAsync(IEnumerable{TopicSpecification}, CreateTopicsOptions)" />
        /// </summary>
        Task<List<CreateTopicResult>> CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.AlterConfigsAsync(Dictionary{ConfigResource, List{ConfigEntry}}, AlterConfigsOptions)" />
        /// </summary>
        Task<List<AlterConfigResult>> AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.AdminClient.DescribeConfigsAsync(IEnumerable{ConfigResource}, DescribeConfigsOptions)" />
        /// </summary>
        Task<List<DescribeConfigResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null);
    }

}
