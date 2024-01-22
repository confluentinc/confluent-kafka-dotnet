// Copyright 2018-2023 Confluent Inc.
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
        ///     DEPRECATED.
        ///     Superseded by ListConsumerGroups and DescribeConsumerGroups.
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
        ///     DEPRECATED.
        ///     Superseded by ListConsumerGroups and DescribeConsumerGroups.
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
        ///     Delete a set of groups.
        /// </summary>
        /// <param name="groups">
        ///     The group names to delete.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting groups.
        /// </param>
        /// <returns>
        ///     The results of the delete group requests.
        /// </returns>
        Task DeleteGroupsAsync(IList<string> groups, DeleteGroupsOptions options = null);

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
        ///     Update the configuration for the specified
        ///     resources. Updates are transactional so if
        ///     on of them may fail then all the of them
        ///     will fail. The configs for a particular
        ///     resource are updated atomically. This operation
        ///     is supported by brokers with version 2.3.0
        ///     or higher. Only specified configuration properties
        ///     will be updated others will stay same as before,
        ///     so there is no need to call DescribleConfigsAsync
        ///     before altering configs.
        ///     Sensitive non-default values are retained after
        ///     IncrementalAlterConfigsAsync unlike
        ///     AlterConfigsAsync.
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
        Task<List<IncrementalAlterConfigsResult>> IncrementalAlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, IncrementalAlterConfigsOptions options = null);


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

        /// <summary>
        ///     Delete records (messages) in topic partitions
        ///     older than the offsets provided.
        /// </summary>
        /// <param name="topicPartitionOffsets">
        ///     The offsets to delete up to.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting records.
        /// </param>
        /// <returns>
        ///     The result of the delete records request.
        /// </returns>
        Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null);

        /// <summary>
        ///     Creates one or more ACL bindings.
        /// </summary>
        /// <param name="aclBindings">
        ///     A IEnumerable with the ACL bindings to create.
        /// </param>
        /// <param name="options">
        ///     The options to use when creating the ACL bindings.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        ///     Thrown if <paramref name="aclBindings"/> param is null
        ///     or a <see cref="Confluent.Kafka.Admin.AclBinding.Entry"/> is null or
        ///     a <see cref="Confluent.Kafka.Admin.AclBinding.Pattern"/> is null.
        /// </exception>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if the <paramref name="aclBindings"/> param is empty.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.CreateAclsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.CreateAclsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task with an empty result when successful.
        /// </returns>
        Task CreateAclsAsync(IEnumerable<AclBinding> aclBindings, CreateAclsOptions options = null);


        /// <summary>
        ///    Finds ACL bindings using a filter.
        /// </summary>
        /// <param name="aclBindingFilter">
        ///    A filter with attributes that must match.
        ///    string attributes match exact values or any string if set to null.
        ///    enum attributes match exact values or any value if equal to `Any`.
        ///    If `ResourcePatternType` is set to <see cref="Admin.ResourcePatternType.Match"/> returns ACL bindings with:
        ///    * <see cref="Admin.ResourcePatternType.Literal"/> pattern type with resource name equal to the given resource name
        ///    * <see cref="Admin.ResourcePatternType.Literal"/> pattern type with wildcard resource name that matches the given resource name
        ///    * <see cref="Admin.ResourcePatternType.Prefixed"/> pattern type with resource name that is a prefix of the given resource name
        /// </param>
        /// <param name="options">
        ///     The options to use when describing ACL bindings.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        ///     Thrown if <paramref name="aclBindingFilter"/> param is null
        ///     or any of <see cref="Confluent.Kafka.Admin.AclBindingFilter.EntryFilter"/> and
        ///     <see cref="Confluent.Kafka.Admin.AclBindingFilter.PatternFilter"/> is null.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.DescribeAclsException">
        ///     Thrown if the corresponding result is in
        ///     error. The entire result is
        ///     available via the <see cref="Confluent.Kafka.Admin.DescribeAclsException.Result" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task returning a <see cref="Admin.DescribeAclsResult"/>.
        /// </returns>
        Task<DescribeAclsResult> DescribeAclsAsync(AclBindingFilter aclBindingFilter, DescribeAclsOptions options = null);

        /// <summary>
        ///    Deletes ACL bindings using multiple filters.
        /// </summary>
        /// <param name="aclBindingFilters">
        ///    A IEnumerable of ACL binding filters to match ACLs to delete.
        ///    string attributes match exact values or any string if set to null.
        ///    enum attributes match exact values or any value if equal to `Any`.
        ///    If `ResourcePatternType` is set to <see cref="Admin.ResourcePatternType.Match"/> deletes ACL bindings with:
        ///    * <see cref="Admin.ResourcePatternType.Literal"/> pattern type with resource name equal to the given resource name
        ///    * <see cref="Admin.ResourcePatternType.Literal"/> pattern type with wildcard resource name that matches the given resource name
        ///    * <see cref="Admin.ResourcePatternType.Prefixed"/> pattern type with resource name that is a prefix of the given resource name
        /// </param>
        /// <param name="options">
        ///     The options to use when describing ACL bindings.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        ///     Thrown if <paramref name="aclBindingFilters"/> param is null
        ///     or any of <see cref="Confluent.Kafka.Admin.AclBindingFilter.EntryFilter"/> and
        ///     <see cref="Confluent.Kafka.Admin.AclBindingFilter.PatternFilter"/> is null.
        /// </exception>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if the <paramref name="aclBindingFilters"/> param is empty.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.DeleteAclsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.DeleteAclsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task returning a List of <see cref="Confluent.Kafka.Admin.DeleteAclsResult"/>.
        /// </returns>
        Task<List<DeleteAclsResult>> DeleteAclsAsync(IEnumerable<AclBindingFilter> aclBindingFilters, DeleteAclsOptions options = null);

        /// <summary>
        ///    Delete committed offsets for a set of partitions in a consumer
        ///    group. This will succeed at the partition level only if the group
        ///    is not actively subscribed to the corresponding topic.
        /// </summary>
        /// <param name="group">
        ///     Consumer group id 
        /// </param>
        /// <param name="partitions">
        ///     Enumerable of topic partitions to delete committed offsets for.
        /// </param>
        /// <param name="options">
        ///     The options to use when deleting the committed offset.
        /// </param>
        /// <returns>
        ///     A Task returning <see cref="Confluent.Kafka.Admin.DeleteConsumerGroupOffsetsResult"/>.
        /// </returns>
        Task<DeleteConsumerGroupOffsetsResult> DeleteConsumerGroupOffsetsAsync(String group, IEnumerable<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options = null);

        /// <summary>
        ///     Alters consumer group offsets for a number of topic partitions.
        /// </summary>
        /// <param name="groupPartitions">
        ///    A IEnumerable of ConsumerGroupTopicPartitionOffsets, each denoting the group and the
        ///    TopicPartitionOffsets associated with that group to alter the offsets for.
        ///    The Count of the IEnumerable must exactly be 1.
        /// </param>
        /// <param name="options">
        ///     The options to use when altering consumer group offsets.
        /// </param>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if the <paramref name="groupPartitions"/> has a count not equal
        ///     to 1, or if any of the topic names are null.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.AlterConsumerGroupOffsetsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.AlterConsumerGroupOffsetsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task returning a List of <see cref="Confluent.Kafka.Admin.AlterConsumerGroupOffsetsResult"/>.
        /// </returns>
        Task<List<AlterConsumerGroupOffsetsResult>> AlterConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitionOffsets> groupPartitions, AlterConsumerGroupOffsetsOptions options = null);

        /// <summary>
        ///    Lists consumer group offsets for a number of topic partitions.
        /// </summary>
        /// <param name="groupPartitions">
        ///    A IEnumerable of ConsumerGroupTopicPartitions, each denoting the group and the
        ///    TopicPartitions associated with that group to fetch the offsets for.
        ///    The Count of the IEnumerable must exactly be 1.
        /// </param>
        /// <param name="options">
        ///     The options to use when listing consumer group offsets.
        /// </param>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if the <paramref name="groupPartitions"/> has a count not equal
        ///     to 1, or if any of the topic names are null.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.ListConsumerGroupOffsetsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.ListConsumerGroupOffsetsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task returning a List of <see cref="Confluent.Kafka.Admin.ListConsumerGroupOffsetsResult"/>.
        /// </returns>
        Task<List<ListConsumerGroupOffsetsResult>> ListConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitions> groupPartitions, ListConsumerGroupOffsetsOptions options = null);

        /// <summary>
        ///    Lists consumer groups in the cluster.
        /// </summary>
        /// <param name="options">
        ///     The options to use while listing consumer groups.
        /// </param>
        /// <exception cref="KafkaException">
        ///     Thrown if there is any client-level error.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.ListConsumerGroupsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.ListConsumerGroupsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A ListConsumerGroupsResult, which contains a List of
        ///     <see cref="Confluent.Kafka.Admin.ConsumerGroupListing"/>.
        /// </returns>
        Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions options = null);

        /// <summary>
        ///    Describes consumer groups in the cluster.
        /// </summary>
        /// <param name="groups">
        ///     The list of groups to describe. This can be set
        ///     to null to describe all groups.
        /// </param>
        /// <param name="options">
        ///     The options to use while describing consumer groups.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if there is any client-level error.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.DescribeConsumerGroupsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.DescribeConsumerGroupsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A List of <see cref="Confluent.Kafka.Admin.ConsumerGroupDescription"/>.
        /// </returns>
        Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(
            IEnumerable<string> groups, DescribeConsumerGroupsOptions options = null);

        /// <summary>
        ///    Describes user SASL/SCRAM credentials.
        /// </summary>
        /// <param name="users">
        ///     The list of users to describe for. This can be set
        ///     to null to describe all users. Individual users
        ///     cannot be empty strings.
        /// </param>
        /// <param name="options">
        ///     The options to use while describing user scram credentials.
        /// </param>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if any requested user is a null or empty string.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.DescribeUserScramCredentialsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.DescribeUserScramCredentialsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A <see cref="Confluent.Kafka.Admin.DescribeUserScramCredentialsResult"/>, which contains a List of
        ///     <see cref="Confluent.Kafka.Admin.UserScramCredentialsDescription"/>.
        /// </returns>
        Task<DescribeUserScramCredentialsResult> DescribeUserScramCredentialsAsync(IEnumerable<string> users, DescribeUserScramCredentialsOptions options = null);

        /// <summary>
        ///     Alter user SASL/SCRAM credentials.
        /// </summary>
        /// <param name="alterations">
        ///     A IEnumerable with alterations to execute.
        /// </param>
        /// <param name="options">
        ///     The options to use when alter user scram credentials.
        /// </param>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if any alteration isn't an instance of
        ///     UserScramCredentialUpsertion or UserScramCredentialDeletion.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.AlterUserScramCredentialsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.AlterUserScramCredentialsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A Task with an empty result when successful.
        /// </returns>
        Task AlterUserScramCredentialsAsync(IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options = null);
    }

    /// <summary>
    ///     Extension methods for default <see cref="IAdminClient"/> implementations.
    /// </summary>
    public static class IAdminClientExtensions
    {
        
        /// <summary>
        ///    Describes topics in the cluster.
        /// </summary>
        /// <param name="adminClient">
        ///     AdminClient interface.
        /// </param>
        /// <param name="topicCollection">
        ///     A collection of topics to describe.
        /// </param>
        /// <param name="options">
        ///     The options to use while describing topics.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if there is any client-level error.
        /// </exception>
        /// <exception cref="Confluent.Kafka.Admin.DescribeTopicsException">
        ///     Thrown if any of the constituent results is in
        ///     error. The entire result (which may contain
        ///     constituent results that are not in error) is
        ///     available via the <see cref="Confluent.Kafka.Admin.DescribeTopicsException.Results" />
        ///     property of the exception.
        /// </exception>
        /// <returns>
        ///     A <see cref="Confluent.Kafka.Admin.DescribeTopicsResult"/>, which contains a List of
        ///     <see cref="Confluent.Kafka.Admin.TopicDescription"/>.
        /// </returns>
        public static Task<DescribeTopicsResult> DescribeTopicsAsync(
            this IAdminClient adminClient,
            TopicCollection topicCollection, DescribeTopicsOptions options = null)
        {
            if (adminClient is AdminClient)
            {
                return ((AdminClient) adminClient).DescribeTopicsAsync(
                    topicCollection, options);
            }
            throw new NotImplementedException();
        }

        /// <summary>
        ///    Describes the cluster.
        /// </summary>
        /// <param name="adminClient">
        ///     AdminClient interface.
        /// </param>
        /// <param name="options">
        ///     The options to use while describing cluster.
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if there is any client-level error.
        /// </exception>
        /// <returns>
        ///     A <see cref="Confluent.Kafka.Admin.DescribeClusterResult"/>.
        /// </returns>
        public static Task<DescribeClusterResult> DescribeClusterAsync(
            this IAdminClient adminClient,
            DescribeClusterOptions options = null)
        {
            if (adminClient is AdminClient)
            {
                return ((AdminClient) adminClient).DescribeClusterAsync(
                    options);
            }
            throw new NotImplementedException();
        }
        
        /// <summary>
        ///     Enables to find the beginning offset,
        ///     end offset as well as the offset matching a timestamp
        ///     or the offset with max timestamp in partitions.
        /// </summary>
        /// <param name="adminClient">
        ///     AdminClient interface.
        /// </param>
        /// <param name="topicPartitionOffsets">
        ///     A IEnumerable with partition to offset pairs (partitions must be unique).
        /// </param>
        /// <param name="options">
        ///     The options to use for this call.
        /// </param>
        public static Task<ListOffsetsResult> ListOffsetsAsync(
            this IAdminClient adminClient,
            IEnumerable<TopicPartitionOffsetSpec> topicPartitionOffsets,
            ListOffsetsOptions options = null)
        {
            if (adminClient is AdminClient)
            {
                return ((AdminClient) adminClient).ListOffsetsAsync(
                        topicPartitionOffsets,
                        options);
            }
            throw new NotImplementedException();
        }
    }


}
