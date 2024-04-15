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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Impl;
using static Confluent.Kafka.Internal.Util.Marshal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements an Apache Kafka admin client.
    /// </summary>
    internal class AdminClient : IAdminClient
    {
        private int cancellationDelayMaxMs;

        private Task callbackTask;
        private CancellationTokenSource callbackCts;

        private IntPtr resultQueue = IntPtr.Zero;

        private List<DeleteRecordsReport> extractDeleteRecordsReports(IntPtr resultPtr)
            => SafeKafkaHandle.GetTopicPartitionOffsetErrorList(resultPtr)
                    .Select(a => new DeleteRecordsReport
                    {
                        Topic = a.Topic,
                        Partition = a.Partition,
                        Offset = a.Offset,
                        Error = a.Error
                    })
                    .ToList();

        private List<CreateTopicReport> extractTopicResults(IntPtr topicResultsPtr, int topicResultsCount)
        {
            IntPtr[] topicResultsPtrArr = new IntPtr[topicResultsCount];
            Marshal.Copy(topicResultsPtr, topicResultsPtrArr, 0, topicResultsCount);

            return topicResultsPtrArr.Select(topicResultPtr => new CreateTopicReport 
                {
                    Topic = PtrToStringUTF8(Librdkafka.topic_result_name(topicResultPtr)),
                    Error = new Error(
                        Librdkafka.topic_result_error(topicResultPtr), 
                        PtrToStringUTF8(Librdkafka.topic_result_error_string(topicResultPtr)))
                }).ToList();
        }

        private ConfigEntryResult extractConfigEntry(IntPtr configEntryPtr)
        {
            var synonyms = new List<ConfigSynonym>();
            var synonymsPtr = Librdkafka.ConfigEntry_synonyms(configEntryPtr, out UIntPtr synonymsCount);
            if (synonymsPtr != IntPtr.Zero)
            {
                IntPtr[] synonymsPtrArr = new IntPtr[(int)synonymsCount];
                Marshal.Copy(synonymsPtr, synonymsPtrArr, 0, (int)synonymsCount);
                synonyms = synonymsPtrArr
                    .Select(synonymPtr => extractConfigEntry(synonymPtr))
                    .Select(e => new ConfigSynonym { Name = e.Name, Value = e.Value, Source = e.Source } )
                    .ToList();
            }

            return new ConfigEntryResult
            {
                Name = PtrToStringUTF8(Librdkafka.ConfigEntry_name(configEntryPtr)),
                Value = PtrToStringUTF8(Librdkafka.ConfigEntry_value(configEntryPtr)),
                IsDefault = (int)Librdkafka.ConfigEntry_is_default(configEntryPtr) == 1,
                IsSensitive = (int)Librdkafka.ConfigEntry_is_sensitive(configEntryPtr) == 1,
                IsReadOnly = (int)Librdkafka.ConfigEntry_is_read_only(configEntryPtr) == 1,
                Source = Librdkafka.ConfigEntry_source(configEntryPtr),
                Synonyms = synonyms
            };
        }

        private List<DescribeConfigsReport> extractResultConfigs(IntPtr configResourcesPtr, int configResourceCount)
        {
            var result = new List<DescribeConfigsReport>();

            IntPtr[] configResourcesPtrArr = new IntPtr[configResourceCount];
            Marshal.Copy(configResourcesPtr, configResourcesPtrArr, 0, configResourceCount);
            foreach (var configResourcePtr in configResourcesPtrArr)
            {
                var resourceName = PtrToStringUTF8(Librdkafka.ConfigResource_name(configResourcePtr));
                var errorCode = Librdkafka.ConfigResource_error(configResourcePtr);
                var errorReason = PtrToStringUTF8(Librdkafka.ConfigResource_error_string(configResourcePtr));
                var resourceConfigType = Librdkafka.ConfigResource_type(configResourcePtr);

                var configEntriesPtr = Librdkafka.ConfigResource_configs(configResourcePtr, out UIntPtr configEntryCount);
                IntPtr[] configEntriesPtrArr = new IntPtr[(int)configEntryCount];
                if ((int)configEntryCount > 0)
                {
                    Marshal.Copy(configEntriesPtr, configEntriesPtrArr, 0, (int)configEntryCount);
                }
                var configEntries = configEntriesPtrArr
                    .Select(configEntryPtr => extractConfigEntry(configEntryPtr))
                    .ToDictionary(e => e.Name);

                result.Add(new DescribeConfigsReport { 
                    ConfigResource = new ConfigResource { Name = resourceName, Type = resourceConfigType },
                    Entries = configEntries,
                    Error = new Error(errorCode, errorReason)
                });
            }

            return result;
        }

        private static List<DeleteGroupReport> extractDeleteGroupsReport(IntPtr eventPtr)
        {
            IntPtr groupsResultPtr = Librdkafka.DeleteGroups_result_groups(eventPtr, out UIntPtr resultCountPtr);
            int groupsResultCount = (int)resultCountPtr;
            IntPtr[] groupsResultPtrArr = new IntPtr[groupsResultCount];
            Marshal.Copy(groupsResultPtr, groupsResultPtrArr, 0, groupsResultCount);

            return groupsResultPtrArr.Select(groupResultPtr => new DeleteGroupReport
            {
                Group = PtrToStringUTF8(Librdkafka.group_result_name(groupResultPtr)),
                Error = new Error(Librdkafka.group_result_error(groupResultPtr), false)
            }).ToList();
        }

        private List<CreateAclReport> extractCreateAclReports(IntPtr aclResultsPtr, int aclResultsCount)
        {
            IntPtr[] aclsResultsPtrArr = new IntPtr[aclResultsCount];
            Marshal.Copy(aclResultsPtr, aclsResultsPtrArr, 0, aclResultsCount);

            return aclsResultsPtrArr.Select(aclResultPtr =>
                new CreateAclReport 
                {
                    Error = new Error(Librdkafka.acl_result_error(aclResultPtr), false)
                }
            ).ToList();
        }

        private List<AclBinding> extractAclBindings(IntPtr aclBindingsPtr, int aclBindingsCnt)
        {
            if (aclBindingsCnt == 0) { return new List<AclBinding> {}; }
            IntPtr[] aclBindingsPtrArr = new IntPtr[aclBindingsCnt];
            Marshal.Copy(aclBindingsPtr, aclBindingsPtrArr, 0, aclBindingsCnt);

            return aclBindingsPtrArr.Select(aclBindingPtr =>
                new AclBinding()
                {
                    Pattern = new ResourcePattern
                    {
                        Type = Librdkafka.AclBinding_restype(aclBindingPtr),
                        Name = PtrToStringUTF8(Librdkafka.AclBinding_name(aclBindingPtr)),
                        ResourcePatternType = Librdkafka.AclBinding_resource_pattern_type(aclBindingPtr)
                    },
                    Entry = new AccessControlEntry
                    {
                        Principal = PtrToStringUTF8(Librdkafka.AclBinding_principal(aclBindingPtr)),
                        Host = PtrToStringUTF8(Librdkafka.AclBinding_host(aclBindingPtr)),
                        Operation = Librdkafka.AclBinding_operation(aclBindingPtr),
                        PermissionType = Librdkafka.AclBinding_permission_type(aclBindingPtr)
                    }
                }
            ).ToList();
        }

        private DescribeAclsReport extractDescribeAclsReport(IntPtr resultPtr)
        {
            var errCode = Librdkafka.event_error(resultPtr);
            var errString = Librdkafka.event_error_string(resultPtr);
            var resultAcls = Librdkafka.DescribeAcls_result_acls(resultPtr,
                                    out UIntPtr resultAclCntPtr);
            return new DescribeAclsReport
            {
                Error = new Error(errCode, errString, false),
                AclBindings = extractAclBindings(resultAcls, (int) resultAclCntPtr)
            };
        }

        private List<DeleteAclsReport> extractDeleteAclsReports(IntPtr resultPtr)
        {
            var resultResponsesPtr = Librdkafka.DeleteAcls_result_responses(resultPtr, out UIntPtr resultResponsesCntPtr);

            IntPtr[] resultResponsesPtrArr = new IntPtr[(int)resultResponsesCntPtr];
            Marshal.Copy(resultResponsesPtr, resultResponsesPtrArr, 0, (int)resultResponsesCntPtr);

            return resultResponsesPtrArr.Select(resultResponsePtr => {
                var matchingAcls = Librdkafka.DeleteAcls_result_response_matching_acls(
                                        resultResponsePtr, out UIntPtr resultResponseAclCntPtr);
                return new DeleteAclsReport 
                {
                    Error = new Error(Librdkafka.DeleteAcls_result_response_error(resultResponsePtr), false),
                    AclBindings = extractAclBindings(matchingAcls, (int) resultResponseAclCntPtr)
                };
            }).ToList();
        }

        private DeleteConsumerGroupOffsetsReport extractDeleteConsumerGroupOffsetsReports(IntPtr resultPtr)
        {
            IntPtr groupsOffsetsResultPtr = Librdkafka.DeleteConsumerGroupOffsets_result_groups(resultPtr, out UIntPtr resultCount);
            int groupsOffsetsResultCount = (int)resultCount;
            IntPtr[] groupsOffsetsResultArr = new IntPtr[groupsOffsetsResultCount];
            Marshal.Copy(groupsOffsetsResultPtr, groupsOffsetsResultArr, 0, groupsOffsetsResultCount);

            return new DeleteConsumerGroupOffsetsReport
            {
                Group = PtrToStringUTF8(Librdkafka.group_result_name(groupsOffsetsResultArr[0])),
                Error = new Error(Librdkafka.group_result_error(groupsOffsetsResultArr[0]), false),
                Partitions = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(Librdkafka.group_result_partitions(groupsOffsetsResultArr[0]))
                    .Select(a => new TopicPartitionOffsetError(a.Topic, a.Partition, a.Offset, a.Error))
                    .ToList()
            };
        }

        private List<ListConsumerGroupOffsetsReport> extractListConsumerGroupOffsetsResults(IntPtr resultPtr)
        {
            var resultGroupsPtr = Librdkafka.ListConsumerGroupOffsets_result_groups(resultPtr, out UIntPtr resultCountPtr);
            IntPtr[] resultGroupsPtrArr = new IntPtr[(int)resultCountPtr];
            Marshal.Copy(resultGroupsPtr, resultGroupsPtrArr, 0, (int)resultCountPtr);

            return resultGroupsPtrArr.Select(resultGroupPtr => {

                // Construct the TopicPartitionOffsetError list from internal list.
                var partitionsPtr = Librdkafka.group_result_partitions(resultGroupPtr);

                return new ListConsumerGroupOffsetsReport {
                    Group = PtrToStringUTF8(Librdkafka.group_result_name(resultGroupPtr)),
                    Error = new Error(Librdkafka.group_result_error(resultGroupPtr), false),
                    Partitions = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitionsPtr),
                };
            }).ToList();
        }

        private List<AlterConsumerGroupOffsetsReport> extractAlterConsumerGroupOffsetsResults(IntPtr resultPtr)
        {
            var resultGroupsPtr = Librdkafka.AlterConsumerGroupOffsets_result_groups(resultPtr, out UIntPtr resultCountPtr);
            IntPtr[] resultGroupsPtrArr = new IntPtr[(int)resultCountPtr];
            Marshal.Copy(resultGroupsPtr, resultGroupsPtrArr, 0, (int)resultCountPtr);

            return resultGroupsPtrArr.Select(resultGroupPtr => {

                // Construct the TopicPartitionOffsetError list from internal list.
                var partitionsPtr = Librdkafka.group_result_partitions(resultGroupPtr);

                return new AlterConsumerGroupOffsetsReport {
                    Group = PtrToStringUTF8(Librdkafka.group_result_name(resultGroupPtr)),
                    Error = new Error(Librdkafka.group_result_error(resultGroupPtr), false),
                    Partitions = SafeKafkaHandle.GetTopicPartitionOffsetErrorList(partitionsPtr),
                };
            }).ToList();
        }

        private ListConsumerGroupsReport extractListConsumerGroupsResults(IntPtr resultPtr)
        {
            var result = new ListConsumerGroupsReport()
            {
                Valid = new List<ConsumerGroupListing>(),
                Errors = new List<Error>(),
            };

            var validResultsPtr = Librdkafka.ListConsumerGroups_result_valid(resultPtr, out UIntPtr resultCountPtr);
            if ((int)resultCountPtr != 0)
            {
                IntPtr[] consumerGroupListingPtrArr = new IntPtr[(int)resultCountPtr];
                Marshal.Copy(validResultsPtr, consumerGroupListingPtrArr, 0, (int)resultCountPtr);
                result.Valid = consumerGroupListingPtrArr.Select(cglPtr => {
                    return new ConsumerGroupListing()
                    {
                        GroupId = PtrToStringUTF8(Librdkafka.ConsumerGroupListing_group_id(cglPtr)),
                        IsSimpleConsumerGroup =
                            (int)Librdkafka.ConsumerGroupListing_is_simple_consumer_group(cglPtr) == 1,
                        State = Librdkafka.ConsumerGroupListing_state(cglPtr),
                    };
                }).ToList();
            }


            var errorsPtr = Librdkafka.ListConsumerGroups_result_errors(resultPtr, out UIntPtr errorCountPtr);
            if ((int)errorCountPtr != 0)
            {
                IntPtr[] errorsPtrArr = new IntPtr[(int)errorCountPtr];
                Marshal.Copy(errorsPtr, errorsPtrArr, 0, (int)errorCountPtr);
                result.Errors = errorsPtrArr.Select(errorPtr => new Error(errorPtr)).ToList();
            }

            return result;
        }

        private DescribeConsumerGroupsReport extractDescribeConsumerGroupsResults(IntPtr resultPtr)
        {
            var groupsPtr = Librdkafka.DescribeConsumerGroups_result_groups(resultPtr, out UIntPtr groupsCountPtr);

            var result = new DescribeConsumerGroupsReport()
            {
                ConsumerGroupDescriptions = new List<ConsumerGroupDescription>()
            };

            if ((int)groupsCountPtr == 0)
                return result;

            IntPtr[] groupPtrArr = new IntPtr[(int)groupsCountPtr];
            Marshal.Copy(groupsPtr, groupPtrArr, 0, (int)groupsCountPtr);

            result.ConsumerGroupDescriptions = groupPtrArr.Select(groupPtr => {

                var coordinatorPtr = Librdkafka.ConsumerGroupDescription_coordinator(groupPtr);
                var coordinator = extractNode(coordinatorPtr);

                var memberCount = (int)Librdkafka.ConsumerGroupDescription_member_count(groupPtr);
                var members = new List<MemberDescription>();
                for (int midx = 0; midx < memberCount; midx++)
                {
                    var memberPtr = Librdkafka.ConsumerGroupDescription_member(groupPtr, (IntPtr)midx);
                    var member = new MemberDescription()
                    {
                        ClientId =
                            PtrToStringUTF8(Librdkafka.MemberDescription_client_id(memberPtr)),
                        ConsumerId =
                            PtrToStringUTF8(Librdkafka.MemberDescription_consumer_id(memberPtr)),
                        Host =
                            PtrToStringUTF8(Librdkafka.MemberDescription_host(memberPtr)),
                        GroupInstanceId =
                            PtrToStringUTF8(Librdkafka.MemberDescription_group_instance_id(memberPtr)),
                    };
                    var assignmentPtr = Librdkafka.MemberDescription_assignment(memberPtr);
                    var topicPartitionPtr = Librdkafka.MemberAssignment_topic_partitions(assignmentPtr);
                    member.Assignment = new MemberAssignment();
                    if (topicPartitionPtr != IntPtr.Zero)
                    {
                        member.Assignment.TopicPartitions = SafeKafkaHandle.GetTopicPartitionList(topicPartitionPtr);
                    }
                    members.Add(member);
                }

                var authorizedOperations = extractAuthorizedOperations(
                    Librdkafka.ConsumerGroupDescription_authorized_operations(groupPtr,
                        out UIntPtr authorizedOperationCount),
                    (int) authorizedOperationCount);

                var desc = new ConsumerGroupDescription()
                {
                    GroupId =
                        PtrToStringUTF8(Librdkafka.ConsumerGroupDescription_group_id(groupPtr)),
                    Error =
                        new Error(Librdkafka.ConsumerGroupDescription_error(groupPtr), false),
                    IsSimpleConsumerGroup =
                        (int)Librdkafka.ConsumerGroupDescription_is_simple_consumer_group(groupPtr) == 1,
                    PartitionAssignor =
                        PtrToStringUTF8(Librdkafka.ConsumerGroupDescription_partition_assignor(groupPtr)),
                    State =
                        Librdkafka.ConsumerGroupDescription_state(groupPtr),
                    Coordinator = coordinator,
                    Members = members,
                    AuthorizedOperations = authorizedOperations,
                };
                return desc;
            }).ToList();

            return result;
        }

        private DescribeUserScramCredentialsReport extractDescribeUserScramCredentialsResult(IntPtr eventPtr)
        {
            var report = new DescribeUserScramCredentialsReport();
            
            var resultDescriptionsPtr = Librdkafka.DescribeUserScramCredentials_result_descriptions(
                eventPtr,
                out UIntPtr resultDescriptionCntPtr);

            IntPtr[] resultDescriptionsPtrArr = new IntPtr[(int)resultDescriptionCntPtr];
            Marshal.Copy(resultDescriptionsPtr, resultDescriptionsPtrArr, 0, (int)resultDescriptionCntPtr);
            
            var descriptions = resultDescriptionsPtrArr.Select(resultDescriptionPtr =>
            {
                var description = new UserScramCredentialsDescription();
                
                var user = PtrToStringUTF8(Librdkafka.UserScramCredentialsDescription_user(resultDescriptionPtr));
                IntPtr cError = Librdkafka.UserScramCredentialsDescription_error(resultDescriptionPtr);
                var error = new Error(cError, false);
                var scramCredentialInfos = new List<ScramCredentialInfo>();
                if (Librdkafka.error_code(cError)==0)
                {
                    int numCredentials = Librdkafka.UserScramCredentialsDescription_scramcredentialinfo_count(resultDescriptionPtr);
                    for(int j=0; j<numCredentials; j++)
                    {
                        var ScramCredentialInfo = new ScramCredentialInfo();
                        IntPtr c_ScramCredentialInfo = Librdkafka.UserScramCredentialsDescription_scramcredentialinfo(resultDescriptionPtr,j);
                        ScramCredentialInfo.Mechanism = Librdkafka.ScramCredentialInfo_mechanism(c_ScramCredentialInfo);
                        ScramCredentialInfo.Iterations = Librdkafka.ScramCredentialInfo_iterations(c_ScramCredentialInfo);
                        scramCredentialInfos.Add(ScramCredentialInfo);
                    }
                }
                
                return new UserScramCredentialsDescription {
                    User = user,
                    Error = error,
                    ScramCredentialInfos = scramCredentialInfos
                };
            }).ToList();
            
            report.UserScramCredentialsDescriptions = descriptions;
            return report;
        }

        private List<AlterUserScramCredentialsReport> extractAlterUserScramCredentialsResults(IntPtr eventPtr)
        {
            var reports = new List<AlterUserScramCredentialsReport>();
            var resultResponsesPtr = Librdkafka.AlterUserScramCredentials_result_responses(
                eventPtr,
                out UIntPtr resultResponsesCntPtr);

            IntPtr[] resultResponsesPtrArr = new IntPtr[(int)resultResponsesCntPtr];
            Marshal.Copy(resultResponsesPtr, resultResponsesPtrArr, 0, (int)resultResponsesCntPtr);
            
            return resultResponsesPtrArr.Select(resultResponsePtr => {
                var user = 
                PtrToStringUTF8(
                    Librdkafka.AlterUserScramCredentials_result_response_user(resultResponsePtr));
                var error =
                    new Error(Librdkafka.AlterUserScramCredentials_result_response_error(resultResponsePtr), false);
                return new AlterUserScramCredentialsReport 
                {
                    User = user,
                    Error = error
                };
            }).ToList();
        }

        private List<TopicPartitionInfo> extractTopicPartitionInfo(IntPtr topicPartitionInfosPtr, int topicPartitionInfosCount)
        {
            if (topicPartitionInfosCount == 0)
                return new List<TopicPartitionInfo>();
                
            IntPtr[] topicPartitionInfos = new IntPtr[topicPartitionInfosCount];
            Marshal.Copy(topicPartitionInfosPtr, topicPartitionInfos, 0, topicPartitionInfosCount);

            return topicPartitionInfos.Select(topicPartitionInfoPtr => {
                return new TopicPartitionInfo
                {
                    ISR = extractNodeList(
                        Librdkafka.TopicPartitionInfo_isr(topicPartitionInfoPtr,
                            out UIntPtr isrCount
                        ),
                        (int) isrCount                        
                    ),
                    Leader = extractNode(Librdkafka.TopicPartitionInfo_leader(topicPartitionInfoPtr)),
                    Partition = Librdkafka.TopicPartitionInfo_partition(topicPartitionInfoPtr),
                    Replicas = extractNodeList(
                        Librdkafka.TopicPartitionInfo_replicas(topicPartitionInfoPtr,
                            out UIntPtr replicasCount
                        ),
                        (int) replicasCount                        
                    ),
                };
            }).ToList();
        }

        private DescribeTopicsReport extractDescribeTopicsResults(IntPtr resultPtr)
        {
            var topicsPtr = Librdkafka.DescribeTopics_result_topics(resultPtr, out UIntPtr topicsCountPtr);

            var result = new DescribeTopicsReport()
            {
                TopicDescriptions = new List<TopicDescription>()
            };

            if ((int)topicsCountPtr == 0)
                return result;

            IntPtr[] topicPtrArr = new IntPtr[(int)topicsCountPtr];
            Marshal.Copy(topicsPtr, topicPtrArr, 0, (int)topicsCountPtr);

            result.TopicDescriptions = topicPtrArr.Select(topicPtr =>
            {

                var topicName = PtrToStringUTF8(Librdkafka.TopicDescription_name(topicPtr));
                var topicId = Librdkafka.TopicDescription_topic_id(topicPtr);
                var error = new Error(Librdkafka.TopicDescription_error(topicPtr), false);
                var isInternal = Librdkafka.TopicDescription_is_internal(topicPtr) != IntPtr.Zero;
                List<AclOperation> authorizedOperations = extractAuthorizedOperations(
                    Librdkafka.TopicDescription_authorized_operations(
                        topicPtr,
                        out UIntPtr authorizedOperationCount),
                    (int) authorizedOperationCount);
                
                return new TopicDescription()
                {
                    Name = topicName,
                    TopicId = extractUuid(topicId),
                    Error = error,
                    AuthorizedOperations = authorizedOperations,
                    IsInternal = isInternal,
                    Partitions = extractTopicPartitionInfo(
                        Librdkafka.TopicDescription_partitions(topicPtr,
                            out UIntPtr partitionsCount),
                        (int) partitionsCount
                    ),
                };
            }).ToList();
            return result;
        }

        private Uuid extractUuid(IntPtr uuidPtr)
        {
            if (uuidPtr == IntPtr.Zero)
            {
                return null;
            }

            return new Uuid(
                Librdkafka.Uuid_most_significant_bits(uuidPtr),
                Librdkafka.Uuid_least_significant_bits(uuidPtr)
            );
        } 
        
        private Node extractNode(IntPtr nodePtr)
        {
            if (nodePtr == IntPtr.Zero)
            {
                return null;
            }
            
            return new Node()
            {
                Id = (int)Librdkafka.Node_id(nodePtr),
                Host = PtrToStringUTF8(Librdkafka.Node_host(nodePtr)),
                Port = (int)Librdkafka.Node_port(nodePtr),
                Rack = PtrToStringUTF8(Librdkafka.Node_rack(nodePtr)),
            };
        }


        private List<Node> extractNodeList(IntPtr nodesPtr, int nodesCount)
        {
            IntPtr[] nodes = new IntPtr[nodesCount];
            Marshal.Copy(nodesPtr, nodes, 0, nodesCount);

            return nodes.Select(nodePtr =>
                extractNode(nodePtr)
            ).ToList();
        }

        private unsafe List<AclOperation> extractAuthorizedOperations(IntPtr authorizedOperationsPtr, int authorizedOperationCount)
        {
            if (authorizedOperationsPtr == IntPtr.Zero)
            {
                return null;
            }
            
            List<AclOperation> authorizedOperations = new List<AclOperation>(authorizedOperationCount);
            for (int i = 0; i < authorizedOperationCount; i++)
            {
                AclOperation *aclOperationPtr = ((AclOperation *) authorizedOperationsPtr.ToPointer()) + i;
                authorizedOperations.Add(
                    *aclOperationPtr);
            }
            return authorizedOperations;
        }

        private DescribeClusterResult extractDescribeClusterResult(IntPtr resultPtr)
        {
            var clusterId = PtrToStringUTF8(Librdkafka.DescribeCluster_result_cluster_id(resultPtr));
            var controller = extractNode(
                    Librdkafka.DescribeCluster_result_controller(resultPtr));

            var nodes = extractNodeList(
                Librdkafka.DescribeCluster_result_nodes(resultPtr, out UIntPtr nodeCount),
                (int) nodeCount);

            List<AclOperation> authorizedOperations = extractAuthorizedOperations(
                Librdkafka.DescribeCluster_result_authorized_operations(
                    resultPtr,
                    out UIntPtr authorizedOperationCount),
                (int) authorizedOperationCount);

            return new DescribeClusterResult()
            {
                ClusterId = clusterId,
                Controller = controller,
                AuthorizedOperations = authorizedOperations,
                Nodes = nodes
            };
        }

        private ListOffsetsReport extractListOffsetsReport(IntPtr resultPtr)
        {
            var resultInfosPtr = Librdkafka.ListOffsets_result_infos(resultPtr, out UIntPtr resulInfosCntPtr);
            
            IntPtr[] resultResponsesPtrArr = new IntPtr[(int)resulInfosCntPtr];
            if ((int)resulInfosCntPtr > 0)
            {
                Marshal.Copy(resultInfosPtr, resultResponsesPtrArr, 0, (int)resulInfosCntPtr);
            }            
            
            ErrorCode reportErrorCode = ErrorCode.NoError;
            var listOffsetsResultInfos = resultResponsesPtrArr.Select(resultResponsePtr => 
            {
                long timestamp = Librdkafka.ListOffsetsResultInfo_timestamp(resultResponsePtr);
                IntPtr c_topic_partition = Librdkafka.ListOffsetsResultInfo_topic_partition(resultResponsePtr);
                var tp = Marshal.PtrToStructure<rd_kafka_topic_partition>(c_topic_partition);
                ErrorCode code = tp.err;
                Error error = new Error(code);
                if ((code != ErrorCode.NoError) && (reportErrorCode == ErrorCode.NoError))
                {
                    reportErrorCode = code;
                }
                return new ListOffsetsResultInfo
                {
                    Timestamp = timestamp,
                    TopicPartitionOffsetError = new TopicPartitionOffsetError(
                        tp.topic,
                        new Partition(tp.partition),
                        new Offset(tp.offset),
                        error)
                };
            }).ToList();

            return new ListOffsetsReport
            {
                ResultInfos = listOffsetsResultInfos,
                Error = new Error(reportErrorCode)
            };
        }

        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            IntPtr eventPtr = IntPtr.Zero;
                            try
                            {
                                eventPtr = kafkaHandle.QueuePoll(resultQueue, this.cancellationDelayMaxMs);
                                if (eventPtr == IntPtr.Zero)
                                {
                                    continue;
                                }

                                var type = Librdkafka.event_type(eventPtr);

                                var ptr = (IntPtr)Librdkafka.event_opaque(eventPtr);
                                var gch = GCHandle.FromIntPtr(ptr);
                                var adminClientResult = gch.Target;
                                gch.Free();

                                if (!adminClientResultTypes.TryGetValue(type, out Type expectedType))
                                {
                                    // Should never happen.
                                    throw new InvalidOperationException($"Unknown result type: {type}");
                                }

                                if (adminClientResult.GetType() != expectedType)
                                {
                                    // Should never happen.
                                    throw new InvalidOperationException($"Completion source type mismatch. Expected {expectedType.Name}, got {type}");
                                }

                                var errorCode = Librdkafka.event_error(eventPtr);
                                var errorStr = Librdkafka.event_error_string(eventPtr);

                                switch (type)
                                {
                                    case Librdkafka.EventType.CreateTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.CreateTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetException(
                                                        new CreateTopicsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<CreateTopicReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteTopics_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractTopicResults(
                                                Librdkafka.DeleteTopics_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                    .Select(r => new DeleteTopicReport { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetException(
                                                        new DeleteTopicsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteTopicReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteGroups_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteGroupReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractDeleteGroupsReport(eventPtr);
                                            
                                            if(result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteGroupReport>>)adminClientResult).TrySetException(
                                                        new DeleteGroupsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteGroupReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.CreatePartitions_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractTopicResults(
                                                    Librdkafka.CreatePartitions_result_topics(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr)
                                                        .Select(r => new CreatePartitionsReport { Topic = r.Topic, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetException(
                                                        new CreatePartitionsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<CreatePartitionsReport>>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DescribeConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.DescribeConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp);

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetException(
                                                        new DescribeConfigsException(result)));
                                            }
                                            else
                                            {
                                                var nr = result.Select(a => new DescribeConfigsResult { ConfigResource = a.ConfigResource, Entries = a.Entries }).ToList();
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DescribeConfigsResult>>)adminClientResult).TrySetResult(nr));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.AlterConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.AlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp)
                                                    .Select(r => new AlterConfigsReport { ConfigResource = r.ConfigResource, Error = r.Error }).ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>)adminClientResult).TrySetException(
                                                        new AlterConfigsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<AlterConfigsReport>>) adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteRecords_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteRecordsReport>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractDeleteRecordsReports(Librdkafka.DeleteRecords_result_offsets(eventPtr));

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteRecordsResult>>)adminClientResult).TrySetException(
                                                        new DeleteRecordsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<DeleteRecordsResult>>)adminClientResult).TrySetResult(
                                                        result.Select(a => new DeleteRecordsResult
                                                            {
                                                                Topic = a.Topic,
                                                                Partition = a.Partition,
                                                                Offset = a.Offset,
                                                                Error = a.Error // internal, not exposed in success case.
                                                            }).ToList()));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.DeleteConsumerGroupOffsets_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<DeleteConsumerGroupOffsetsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractDeleteConsumerGroupOffsetsReports(eventPtr);

                                            if (result.Error.IsError || result.Partitions.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<DeleteConsumerGroupOffsetsResult>)adminClientResult).TrySetException(
                                                        new DeleteConsumerGroupOffsetsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<DeleteConsumerGroupOffsetsResult>)adminClientResult).TrySetResult(
                                                        new DeleteConsumerGroupOffsetsResult
                                                        {
                                                            Group = result.Group,
                                                            Partitions = result.Partitions.Select(r => new TopicPartition(r.Topic, r.Partition)).ToList(),
                                                            Error = result.Error // internal, not exposed in success case.
                                                        }));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.CreateAcls_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<Null>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var reports = extractCreateAclReports(
                                                Librdkafka.CreateAcls_result_acls(eventPtr, out UIntPtr resultCountPtr), (int)resultCountPtr
                                            );

                                            if (reports.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<Null>)adminClientResult).TrySetException(
                                                        new CreateAclsException(reports)));
                                            }
                                            else
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<Null>)adminClientResult).TrySetResult(null));
                                            }
                                        }
                                        break;
                                    case Librdkafka.EventType.DescribeAcls_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<DescribeAclsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var report = extractDescribeAclsReport(eventPtr);

                                            if (report.Error.IsError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<DescribeAclsResult>)adminClientResult).TrySetException(
                                                        new DescribeAclsException(report)));
                                            }
                                            else
                                            {
                                                var result = new DescribeAclsResult
                                                {
                                                    AclBindings = report.AclBindings
                                                };
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<DescribeAclsResult>)adminClientResult).TrySetResult(result));
                                            }
                                        }
                                        break; 
                                    case Librdkafka.EventType.DeleteAcls_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteAclsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var reports = extractDeleteAclsReports(eventPtr);

                                            if (reports.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteAclsResult>>)adminClientResult).TrySetException(
                                                        new DeleteAclsException(reports)));
                                            }
                                            else
                                            {
                                                var results = reports.Select(report => new DeleteAclsResult
                                                    {
                                                        AclBindings = report.AclBindings
                                                    }).ToList();
                                                Task.Run(() => 
                                                    ((TaskCompletionSource<List<DeleteAclsResult>>)adminClientResult).TrySetResult(results));
                                            }
                                        }
                                        break; 

                                    case Librdkafka.EventType.AlterConsumerGroupOffsets_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<List<AlterConsumerGroupOffsetsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractAlterConsumerGroupOffsetsResults(eventPtr);
                                        if (results.Any(r => r.Error.IsError) || results.Any(r => r.Partitions.Any(p => p.Error.IsError)))
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<List<AlterConsumerGroupOffsetsResult>>)adminClientResult).TrySetException(
                                                        new AlterConsumerGroupOffsetsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<List<AlterConsumerGroupOffsetsResult>>)adminClientResult).TrySetResult(
                                                    results
                                                        .Select(r => new AlterConsumerGroupOffsetsResult()
                                                        {
                                                            Group = r.Group,
                                                            Partitions = r.Partitions
                                                        })
                                                        .ToList()
                                                ));
                                        }
                                        break;
                                    }
                                    
                                    case Librdkafka.EventType.IncrementalAlterConfigs_Result:
                                        {
                                            if (errorCode != ErrorCode.NoError)
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<IncrementalAlterConfigsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                            }

                                            var result = extractResultConfigs(
                                                Librdkafka.IncrementalAlterConfigs_result_resources(eventPtr, out UIntPtr cntp), (int)cntp)
                                                    .Select(r => new IncrementalAlterConfigsReport { ConfigResource = r.ConfigResource, Error = r.Error })
                                                    .ToList();

                                            if (result.Any(r => r.Error.IsError))
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<IncrementalAlterConfigsResult>>)adminClientResult).TrySetException(
                                                        new IncrementalAlterConfigsException(result)));
                                            }
                                            else
                                            {
                                                Task.Run(() =>
                                                    ((TaskCompletionSource<List<IncrementalAlterConfigsResult>>) adminClientResult).TrySetResult(
                                                            result.Select(r => new IncrementalAlterConfigsResult
                                                            {
                                                               ConfigResource = r.ConfigResource,
                                                            }).ToList()
                                                    ));
                                            }
                                        }
                                        break;

                                    case Librdkafka.EventType.ListConsumerGroupOffsets_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<List<ListConsumerGroupOffsetsResult>>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractListConsumerGroupOffsetsResults(eventPtr);
                                        if (results.Any(r => r.Error.IsError) || results.Any(r => r.Partitions.Any(p => p.Error.IsError)))
                                        {
                                             Task.Run(() =>
                                                    ((TaskCompletionSource<List<ListConsumerGroupOffsetsResult>>)adminClientResult).TrySetException(
                                                        new ListConsumerGroupOffsetsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<List<ListConsumerGroupOffsetsResult>>)adminClientResult).TrySetResult(
                                                    results
                                                        .Select(r => new ListConsumerGroupOffsetsResult() { Group = r.Group, Partitions = r.Partitions })
                                                        .ToList()
                                                ));
                                        }
                                        break;
                                    }

                                    case Librdkafka.EventType.ListConsumerGroups_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<ListConsumerGroupsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractListConsumerGroupsResults(eventPtr);
                                        if (results.Errors.Count() != 0)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<ListConsumerGroupsResult>)adminClientResult).TrySetException(
                                                        new ListConsumerGroupsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<ListConsumerGroupsResult>)adminClientResult).TrySetResult(
                                                    new ListConsumerGroupsResult() { Valid = results.Valid }
                                                ));
                                        }
                                        break;
                                    }

                                    case Librdkafka.EventType.DescribeConsumerGroups_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeConsumerGroupsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractDescribeConsumerGroupsResults(eventPtr);
                                        if (results.ConsumerGroupDescriptions.Any(desc => desc.Error.IsError))
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeConsumerGroupsResult>)adminClientResult).TrySetException(
                                                        new DescribeConsumerGroupsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<DescribeConsumerGroupsResult>)adminClientResult).TrySetResult(
                                                    new DescribeConsumerGroupsResult() { ConsumerGroupDescriptions = results.ConsumerGroupDescriptions }
                                                ));
                                        }
                                        break;
                                    }
                                    case Librdkafka.EventType.DescribeUserScramCredentials_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeUserScramCredentialsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractDescribeUserScramCredentialsResult(eventPtr);
                                        if (results.UserScramCredentialsDescriptions.Any(desc => desc.Error.IsError))
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeUserScramCredentialsResult>)adminClientResult).TrySetException(
                                                        new DescribeUserScramCredentialsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<DescribeUserScramCredentialsResult>)adminClientResult).TrySetResult(
                                                    new DescribeUserScramCredentialsResult() { UserScramCredentialsDescriptions = results.UserScramCredentialsDescriptions }
                                                ));
                                        }
                                        break;

                                    }
                                    case Librdkafka.EventType.AlterUserScramCredentials_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<Null>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        
                                        var results = extractAlterUserScramCredentialsResults(eventPtr);

                                        if (results.Any(r => r.Error.IsError))
                                        {
                                            Task.Run(() => 
                                                ((TaskCompletionSource<Null>)adminClientResult).TrySetException(
                                                    new AlterUserScramCredentialsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() => 
                                                ((TaskCompletionSource<Null>)adminClientResult).TrySetResult(null));
                                        }
                                        
                                        break;
                                    }
                                    case Librdkafka.EventType.DescribeTopics_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeTopicsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var results = extractDescribeTopicsResults(eventPtr);
                                        if (results.TopicDescriptions.Any(desc => desc.Error.IsError))
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeTopicsResult>)adminClientResult).TrySetException(
                                                        new DescribeTopicsException(results)));
                                        }
                                        else
                                        {
                                            Task.Run(() =>
                                                ((TaskCompletionSource<DescribeTopicsResult>)adminClientResult).TrySetResult(
                                                    new DescribeTopicsResult() { TopicDescriptions = results.TopicDescriptions }
                                                ));
                                        }
                                        break;
                                    }
                                    case Librdkafka.EventType.DescribeCluster_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<DescribeClusterResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        var res = extractDescribeClusterResult(eventPtr);
                                        Task.Run(() =>
                                            ((TaskCompletionSource<DescribeClusterResult>)adminClientResult).TrySetResult(res));
                                        break;
                                    }
                                    case Librdkafka.EventType.ListOffsets_Result:
                                    {
                                        if (errorCode != ErrorCode.NoError)
                                        {
                                            Task.Run(() =>
                                                    ((TaskCompletionSource<ListOffsetsResult>)adminClientResult).TrySetException(
                                                        new KafkaException(kafkaHandle.CreatePossiblyFatalError(errorCode, errorStr))));
                                                break;
                                        }
                                        ListOffsetsReport report = extractListOffsetsReport(eventPtr);
                                        if (report.Error.IsError)
                                        {
                                            Task.Run(() => 
                                                ((TaskCompletionSource<ListOffsetsResult>)adminClientResult).TrySetException(
                                                    new ListOffsetsException(report)));
                                        }
                                        else
                                        {
                                            var result = new ListOffsetsResult() { ResultInfos = report.ResultInfos };
                                            Task.Run(() =>
                                                ((TaskCompletionSource<ListOffsetsResult>)adminClientResult).TrySetResult(
                                                    result));
                                        }
                                        break;
                                    }
                                    default:
                                        // Should never happen.
                                        throw new InvalidOperationException($"Unknown result type: {type}");
                                }
                            }
                            catch
                            {
                                // TODO: If this occurs, it means there's an application logic error
                                //       (i.e. program execution should never get here). Rather than
                                //       ignore the situation, we panic, destroy the librdkafka handle, 
                                //       and exit the polling loop. Further usage of the AdminClient will
                                //       result in exceptions. People will be sure to notice and tell us.
                                this.DisposeResources();
                                break;
                            }
                            finally
                            {
                                if (eventPtr != IntPtr.Zero)
                                {
                                    Librdkafka.event_destroy(eventPtr);
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        internal static Dictionary<Librdkafka.EventType, Type> adminClientResultTypes = new Dictionary<Librdkafka.EventType, Type>
        {
            { Librdkafka.EventType.CreateTopics_Result, typeof(TaskCompletionSource<List<CreateTopicReport>>) },
            { Librdkafka.EventType.DeleteTopics_Result, typeof(TaskCompletionSource<List<DeleteTopicReport>>) },
            { Librdkafka.EventType.DescribeConfigs_Result, typeof(TaskCompletionSource<List<DescribeConfigsResult>>) },
            { Librdkafka.EventType.AlterConfigs_Result, typeof(TaskCompletionSource<List<AlterConfigsReport>>) },
            { Librdkafka.EventType.IncrementalAlterConfigs_Result, typeof(TaskCompletionSource<List<IncrementalAlterConfigsResult>>) },
            { Librdkafka.EventType.CreatePartitions_Result, typeof(TaskCompletionSource<List<CreatePartitionsReport>>) },
            { Librdkafka.EventType.DeleteRecords_Result, typeof(TaskCompletionSource<List<DeleteRecordsResult>>) },
            { Librdkafka.EventType.DeleteConsumerGroupOffsets_Result, typeof(TaskCompletionSource<DeleteConsumerGroupOffsetsResult>) },
            { Librdkafka.EventType.DeleteGroups_Result, typeof(TaskCompletionSource<List<DeleteGroupReport>>) },
            { Librdkafka.EventType.CreateAcls_Result, typeof(TaskCompletionSource<Null>) },
            { Librdkafka.EventType.DescribeAcls_Result, typeof(TaskCompletionSource<DescribeAclsResult>) },
            { Librdkafka.EventType.DeleteAcls_Result, typeof(TaskCompletionSource<List<DeleteAclsResult>>) },
            { Librdkafka.EventType.AlterConsumerGroupOffsets_Result, typeof(TaskCompletionSource<List<AlterConsumerGroupOffsetsResult>>) },
            { Librdkafka.EventType.ListConsumerGroupOffsets_Result, typeof(TaskCompletionSource<List<ListConsumerGroupOffsetsResult>>) },
            { Librdkafka.EventType.ListConsumerGroups_Result, typeof(TaskCompletionSource<ListConsumerGroupsResult>) },
            { Librdkafka.EventType.DescribeConsumerGroups_Result, typeof(TaskCompletionSource<DescribeConsumerGroupsResult>) },
            { Librdkafka.EventType.DescribeUserScramCredentials_Result, typeof(TaskCompletionSource<DescribeUserScramCredentialsResult>) },
            { Librdkafka.EventType.AlterUserScramCredentials_Result, typeof(TaskCompletionSource<Null>) },
            { Librdkafka.EventType.DescribeTopics_Result, typeof(TaskCompletionSource<DescribeTopicsResult>) },
            { Librdkafka.EventType.DescribeCluster_Result, typeof(TaskCompletionSource<DescribeClusterResult>) },
            { Librdkafka.EventType.ListOffsets_Result, typeof(TaskCompletionSource<ListOffsetsResult>) },
        };


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DescribeConfigsAsync(IEnumerable{ConfigResource}, DescribeConfigsOptions)" />
        /// </summary>
        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DescribeConfigResult>> DescribeConfigsConcurrent(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DescribeConfigsResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeConfigs(
                resources, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.AlterConfigsAsync(Dictionary{ConfigResource, List{ConfigEntry}}, AlterConfigsOptions)" />
        /// </summary>
        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {

            var completionSource = new TaskCompletionSource<List<AlterConfigsReport>>();
            // Note: There is a level of indirection between the GCHandle and
            // physical memory address. GCHandle.ToIntPtr doesn't return the
            // physical address, it returns an id that refers to the object via
            // a handle-table.
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.AlterConfigs(
                configs, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.IncrementalAlterConfigsAsync(Dictionary{ConfigResource, List{ConfigEntry}}, IncrementalAlterConfigsOptions)" />
        /// </summary>
        public Task<List<IncrementalAlterConfigsResult>> IncrementalAlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, IncrementalAlterConfigsOptions options = null)
        {

            var completionSource = new TaskCompletionSource<List<IncrementalAlterConfigsResult>>();
            // Note: There is a level of indirection between the GCHandle and
            // physical memory address. GCHandle.ToIntPtr doesn't return the
            // physical address, it returns an id that refers to the object via
            // a handle-table.
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.IncrementalAlterConfigs(
                configs, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.CreateTopicsAsync(IEnumerable{TopicSpecification}, CreateTopicsOptions)" />
        /// </summary>
        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // public List<Task<CreateTopicResult>> CreateTopicsConcurrent(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreateTopicReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreateTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteTopicsAsync(IEnumerable{string}, DeleteTopicsOptions)" />
        /// </summary>
        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<DeleteTopicResult>> DeleteTopicsConcurrent(IEnumerable<string> topics, DeleteTopicsOptions options = null)

            var completionSource = new TaskCompletionSource<List<DeleteTopicReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteTopics(
                topics, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteGroupsAsync(IList{string}, DeleteGroupsOptions)" />
        /// </summary>
        public Task DeleteGroupsAsync(IList<string> groups, DeleteGroupsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<List<DeleteGroupReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteGroups(
                groups, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteConsumerGroupOffsetsAsync(String, IEnumerable{TopicPartition}, DeleteConsumerGroupOffsetsOptions)" />
        /// </summary>
        public Task<DeleteConsumerGroupOffsetsResult> DeleteConsumerGroupOffsetsAsync(String group, IEnumerable<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DeleteConsumerGroupOffsetsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteConsumerGroupOffsets(
                group, partitions, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.CreatePartitionsAsync(IEnumerable{PartitionsSpecification}, CreatePartitionsOptions)" />
        /// </summary>
        public Task CreatePartitionsAsync(
            IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            // TODO: To support results that may complete at different times, we may also want to implement:
            // List<Task<CreatePartitionResult>> CreatePartitionsConcurrent(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)

            var completionSource = new TaskCompletionSource<List<CreatePartitionsReport>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreatePartitions(
                partitionsSpecifications, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteRecordsAsync(IEnumerable{TopicPartitionOffset}, DeleteRecordsOptions)" />
        /// </summary>
        public Task<List<DeleteRecordsResult>> DeleteRecordsAsync(
            IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<List<DeleteRecordsResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteRecords(
                topicPartitionOffsets, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        private IClient ownedClient;
        private Handle handle;

        private SafeKafkaHandle kafkaHandle
            => handle.LibrdkafkaHandle;


        /// <summary>
        ///     Initialize a new AdminClient instance.
        /// </summary>
        /// <param name="handle">
        ///     An underlying librdkafka client handle that the AdminClient will use to 
        ///     make broker requests. It is valid to provide either a Consumer, Producer
        ///     or AdminClient handle.
        /// </param>
        internal AdminClient(Handle handle)
        {
            Config.ExtractCancellationDelayMaxMs(new AdminClientConfig(), out this.cancellationDelayMaxMs);                          
            this.ownedClient = null;
            this.handle = handle;
            Init();
        }

        internal AdminClient(AdminClientBuilder builder)
        {
            var config = Config.ExtractCancellationDelayMaxMs(builder.Config, out this.cancellationDelayMaxMs);

            if (config.Where(prop => prop.Key.StartsWith("dotnet.producer.")).Count() > 0 ||
                config.Where(prop => prop.Key.StartsWith("dotnet.consumer.")).Count() > 0)
            {
                throw new ArgumentException("AdminClient configuration must not include producer or consumer specific configuration properties.");
            }

            // build a producer instance to use as the underlying client.
            var producerBuilder = new ProducerBuilder<Null, Null>(config);
            if (builder.LogHandler != null) { producerBuilder.SetLogHandler((_, logMessage) => builder.LogHandler(this, logMessage)); }
            if (builder.ErrorHandler != null) { producerBuilder.SetErrorHandler((_, error) => builder.ErrorHandler(this, error)); }
            if (builder.StatisticsHandler != null) { producerBuilder.SetStatisticsHandler((_, stats) => builder.StatisticsHandler(this, stats)); }
            if (builder.OAuthBearerTokenRefreshHandler != null) { producerBuilder.SetOAuthBearerTokenRefreshHandler(builder.OAuthBearerTokenRefreshHandler); }
            this.ownedClient = producerBuilder.Build();
            
            this.handle = new Handle
            { 
                Owner = this,
                LibrdkafkaHandle = ownedClient.Handle.LibrdkafkaHandle
            };

            Init();
        }

        private void Init()
        {
            resultQueue = kafkaHandle.CreateQueue();

            callbackCts = new CancellationTokenSource();
            callbackTask = StartPollTask(callbackCts.Token);
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListGroups(TimeSpan)" />
        /// </summary>
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListGroup(string, TimeSpan)" />
        /// </summary>
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.GetMetadata(TimeSpan)" />
        /// </summary>
        public Metadata GetMetadata(TimeSpan timeout)
            => kafkaHandle.GetMetadata(true, null, timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.GetMetadata(string, TimeSpan)" />
        /// </summary>
        public Metadata GetMetadata(string topic, TimeSpan timeout)
            => kafkaHandle.GetMetadata(false, kafkaHandle.newTopic(topic, IntPtr.Zero), timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.AddBrokers(string)" />
        /// </summary>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);

        /// <inheritdoc/>
        public void SetSaslCredentials(string username, string password)
            => kafkaHandle.SetSaslCredentials(username, password);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.Name" />
        /// </summary>
        public string Name
            => kafkaHandle.Name;


        /// <summary>
        ///     An opaque reference to the underlying librdkafka 
        ///     client instance.
        /// </summary>
        public Handle Handle
            => handle;


        /// <summary>
        ///     Releases all resources used by this AdminClient. In the current
        ///     implementation, this method may block for up to 100ms. This 
        ///     will be replaced with a non-blocking version in the future.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.AdminClient" />
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                callbackCts.Cancel();
                try
                {
                    callbackTask.Wait();
                }
                catch (AggregateException e)
                {
                    if (e.InnerException.GetType() != typeof(TaskCanceledException))
                    {
                        // program execution should never get here.
                        throw e.InnerException;
                    }
                }
                finally
                {
                    callbackCts.Dispose();
                }

                DisposeResources();
            }
        }


        private void DisposeResources()
        {
            kafkaHandle.DestroyQueue(resultQueue);

            if (handle.Owner == this)
            {
                ownedClient.Dispose();
            }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.CreateAclsAsync(IEnumerable{AclBinding}, CreateAclsOptions)" />
        /// </summary>
        public Task CreateAclsAsync(IEnumerable<AclBinding> aclBindings, CreateAclsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<Null>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.CreateAcls(
                aclBindings, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DescribeAclsAsync(AclBindingFilter, DescribeAclsOptions)" />
        /// </summary>
        public Task<DescribeAclsResult> DescribeAclsAsync(AclBindingFilter aclBindingFilter, DescribeAclsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DescribeAclsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeAcls(
                aclBindingFilter, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DeleteAclsAsync(IEnumerable{AclBindingFilter}, DeleteAclsOptions)" />
        /// </summary>
        public Task<List<DeleteAclsResult>> DeleteAclsAsync(IEnumerable<AclBindingFilter> aclBindingFilters, DeleteAclsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<List<DeleteAclsResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DeleteAcls(
                aclBindingFilters, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.AlterConsumerGroupOffsetsAsync(IEnumerable{ConsumerGroupTopicPartitionOffsets}, AlterConsumerGroupOffsetsOptions)" />
        /// </summary>
        public Task<List<AlterConsumerGroupOffsetsResult>> AlterConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitionOffsets> groupPartitions, AlterConsumerGroupOffsetsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<List<AlterConsumerGroupOffsetsResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.AlterConsumerGroupOffsets(
                groupPartitions, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListConsumerGroupOffsetsAsync(IEnumerable{ConsumerGroupTopicPartitions}, ListConsumerGroupOffsetsOptions)" />
        /// </summary>
        public Task<List<ListConsumerGroupOffsetsResult>> ListConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitions> groupPartitions, ListConsumerGroupOffsetsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<List<ListConsumerGroupOffsetsResult>>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.ListConsumerGroupOffsets(
                groupPartitions, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.ListConsumerGroupsAsync(ListConsumerGroupsOptions)" />
        /// </summary>
        public Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<ListConsumerGroupsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.ListConsumerGroups(
                options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DescribeConsumerGroupsAsync(IEnumerable{string}, DescribeConsumerGroupsOptions)" />
        /// </summary>
        public Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(IEnumerable<string> groups, DescribeConsumerGroupsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DescribeConsumerGroupsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeConsumerGroups(
                groups, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.DescribeUserScramCredentialsAsync(IEnumerable{string}, DescribeUserScramCredentialsOptions)" />
        /// </summary>
        public Task<DescribeUserScramCredentialsResult> DescribeUserScramCredentialsAsync(IEnumerable<string> users, DescribeUserScramCredentialsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DescribeUserScramCredentialsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeUserScramCredentials(
                users, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClient.AlterUserScramCredentialsAsync(IEnumerable{UserScramCredentialAlteration}, AlterUserScramCredentialsOptions)" />
        /// </summary>
        public Task AlterUserScramCredentialsAsync(IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<Null>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.AlterUserScramCredentials(
                alterations, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClientExtensions.DescribeTopicsAsync(IAdminClient, TopicCollection, DescribeTopicsOptions)" />
        /// </summary>
        public Task<DescribeTopicsResult> DescribeTopicsAsync(TopicCollection topicCollection, DescribeTopicsOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DescribeTopicsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeTopics(
                topicCollection, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClientExtensions.DescribeClusterAsync(IAdminClient, DescribeClusterOptions)" />
        /// </summary>
        public Task<DescribeClusterResult> DescribeClusterAsync(DescribeClusterOptions options = null)
        {
            var completionSource = new TaskCompletionSource<DescribeClusterResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.DescribeCluster(
                options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IAdminClientExtensions.ListOffsetsAsync(IAdminClient, IEnumerable{TopicPartitionOffsetSpec}, ListOffsetsOptions)" />
        /// </summary>
        public Task<ListOffsetsResult> ListOffsetsAsync(IEnumerable<TopicPartitionOffsetSpec> topicPartitionOffsetSpecs,ListOffsetsOptions options = null) {
            var completionSource = new TaskCompletionSource<ListOffsetsResult>();
            var gch = GCHandle.Alloc(completionSource);
            Handle.LibrdkafkaHandle.ListOffsets(
                topicPartitionOffsetSpecs, options, resultQueue,
                GCHandle.ToIntPtr(gch));
            return completionSource.Task;
        }
    }
}
