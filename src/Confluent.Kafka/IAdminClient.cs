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
        ///     Get information pertaining to all groups in
        ///     the Kafka cluster (blocking)
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated
        ///     with this functionality is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        List<GroupInfo> ListGroups(TimeSpan timeout);


        /// <summary>
        ///     Get information pertaining to a particular
        ///     group in the Kafka cluster (blocking).
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated
        ///     with this functionality is subject to change.
        /// </summary>
        /// <param name="group">
        ///     The group of interest.
        /// </param>
        /// <param name="timeout">
        ///     The maximum period of time the call
        ///     may block.
        /// </param>
        /// <returns>
        ///     Returns information pertaining to the
        ///     specified group or null if this group does
        ///     not exist.
        /// </returns>
        GroupInfo ListGroup(string group, TimeSpan timeout);


        /// <summary>
        ///     Query the cluster for metadata for a
        ///     specific topic.
        /// 
        ///     [API-SUBJECT-TO-CHANGE] - The API associated
        ///     with this functionality is subject to change.
        /// </summary>

        Metadata GetMetadata(string topic, TimeSpan timeout);


        /// <summary>
        ///     Query the cluster for metadata.
        ///
        ///     [API-SUBJECT-TO-CHANGE] - The API associated
        ///     with this functionality is subject to change.
        /// </summary>
        Metadata GetMetadata(TimeSpan timeout);


        /// <summary>
        ///     Increase the number of partitions for one
        ///     or more topics as per the supplied
        ///     PartitionsSpecifications.
        /// </summary>
        /// <param name="partitionsSpecifications">
        ///     A collection of PartitionsSpecifications.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating
        ///     the partitions.
        /// </param>
        /// <returns>
        ///     The results of the
        ///     PartitionsSpecification requests.
        /// </returns>
        Task CreatePartitionsAsync(
            IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null);


        /// <summary>
        ///     Delete a set of topics. This operation is not
        ///     transactional so it may succeed for some
        ///     topics while fail for others. It may take
        ///     several seconds after the DeleteTopicsResult
        ///     returns success for all the brokers to become
        ///     aware that the topics are gone. During this
        ///     time, topics may continue to be visible via
        ///     admin operations. If delete.topic.enable is
        ///     false on the brokers, DeleteTopicsAsync will
        ///     mark the topics for deletion, but not
        ///     actually delete them. The Task will return
        ///     successfully in this case.
        /// </summary>
        /// <param name="topics">
        ///     The topic names to delete.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting topics.
        /// </param>
        /// <returns>
        ///     The results of the delete topic requests.
        /// </returns>
        Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null);


        /// <summary>
        ///     Create a set of new topics.
        /// </summary>
        /// <param name="topics">
        ///     A collection of specifications for
        ///     the new topics to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating
        ///     the topics.
        /// </param>
        /// <returns>
        ///     The results of the create topic requests.
        /// </returns>
        Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null);


        /// <summary>
        ///     Update the configuration for the specified
        ///     resources. Updates are not transactional so
        ///     they may succeed for some resources while fail
        ///     for others. The configs for a particular
        ///     resource are updated atomically. This operation
        ///     is supported by brokers with version 0.11.0
        ///     or higher. IMPORTANT NOTE: Unspecified
        ///     configuration properties will be reverted to
        ///     their default values. Furthermore, if you use
        ///     DescribeConfigsAsync to obtain the current set
        ///     of configuration values, modify them, then use 
        ///     AlterConfigsAsync to set them, you will loose
        ///     any non-default values that are marked as
        ///     sensitive because they are not provided by
        ///     DescribeConfigsAsync.
        /// </summary>
        /// <param name="configs">
        ///     The resources with their configs
        ///     (topic is the only resource type with configs
        ///     that can be updated currently).
        /// </param>
        /// <param name="options">
        ///     The options to use when altering configs.
        /// </param>
        /// <returns>
        ///     The results of the alter configs requests.
        /// </returns>
        Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null);


        /// <summary>
        ///     Get the configuration for the specified
        ///     resources. The returned  configuration includes
        ///     default values and the IsDefault property can be
        ///     used to distinguish them from user supplied values.
        ///     The value of config entries where IsSensitive is
        ///     true is always null so that sensitive information
        ///     is not disclosed. Config entries where IsReadOnly
        ///     is true cannot be updated. This operation is
        ///     supported by brokers with version 0.11.0.0 or higher.
        /// </summary>
        /// <param name="resources">
        ///     The resources (topic and broker resource
        ///     types are currently supported)
        /// </param>
        /// <param name="options">
        ///     The options to use when describing configs.
        /// </param>
        /// <returns>
        ///     Configs for the specified resources.
        /// </returns>
        Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null);
    }

}
