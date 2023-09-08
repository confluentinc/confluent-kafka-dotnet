// Copyright 2022 Confluent Inc.
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
using Xunit;
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {

        /// <summary>
        ///     Test functionality of AdminClient.DescribeTopics and
        ///     We test three cases:
        ///     1. For a non-existing topic.
        ///     1. Without creating Acls and with IncludeAuthorizedOperations.
        ///     2. After creating Acls and with IncludeAuthorizedOperations.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DescribeTopics(string bootstrapServers)
        {
            LogToFile("start AdminClient_DescribeTopics");

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig {
                BootstrapServers = bootstrapServers }).Build())
            {
                var describeOptionsWithTimeout = new Admin.DescribeTopicsOptions() { 
                    RequestTimeout = TimeSpan.FromSeconds(30), 
                    IncludeAuthorizedOperations = false
                };
                var describeOptionsWithAuthOps = new Admin.DescribeTopicsOptions() { 
                    RequestTimeout = TimeSpan.FromSeconds(30), 
                    IncludeAuthorizedOperations = true
                };

                var nonexistTopic = Guid.NewGuid().ToString();
                var topicList = new List<string>
                {
                    nonexistTopic,
                    singlePartitionTopic
                };

                try
                {
                    var descResult = adminClient.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames(topicList),
                        describeOptionsWithTimeout).Result;
                }
                catch(AggregateException ex){
                    var desTE = (DescribeTopicsException) ex.InnerException;
                    var resCount = desTE.Results.TopicDescriptions.Count;
                    Assert.True(resCount == 2);
                    Assert.Empty(desTE.Results.TopicDescriptions[0].AuthorizedOperations);
                    Assert.Empty(desTE.Results.TopicDescriptions[1].AuthorizedOperations);
                    Assert.True(desTE.Results.TopicDescriptions[0].Error.IsError);
                    Assert.True(!(desTE.Results.TopicDescriptions[1].Error.IsError));
                }

                var topicListAuthOps = 
                TopicCollection.OfTopicNames(
                    new List<string>
                    {
                        singlePartitionTopic
                    }
                );
                var descResWithAuthOps = adminClient.DescribeTopicsAsync(
                    topicListAuthOps,
                    describeOptionsWithAuthOps).Result;
                Assert.NotEmpty(descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations);
                var initialCount = descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations.Count;
                LogToFile($"{initialCount} initial");

                var topicACLs =  new List<AclBinding>
                {
                    new AclBinding()
                    {
                        Pattern = new ResourcePattern
                        {
                            Type = ResourceType.Topic,
                            Name = singlePartitionTopic,
                            ResourcePatternType = ResourcePatternType.Literal
                        },
                        Entry = new AccessControlEntry
                        {
                            Principal = "User:ANONYMOUS",
                            Host =  "*",
                            Operation = AclOperation.Read,
                            PermissionType = AclPermissionType.Allow
                        }
                    },
                };

                var createAclsOptions = new CreateAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                var deleteAclsOptions = new DeleteAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                var createAclsException = adminClient.CreateAclsAsync(topicACLs, createAclsOptions).Exception;

                descResWithAuthOps = adminClient.DescribeTopicsAsync(topicListAuthOps, describeOptionsWithAuthOps).Result;
                Assert.NotEmpty(descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations);
                var finalCount = descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations.Count;
                LogToFile($"{finalCount} final");

                topicACLs =  new List<AclBinding>
                {
                    new AclBinding()
                    {
                        Pattern = new ResourcePattern
                        {
                            Type = ResourceType.Topic,
                            Name = singlePartitionTopic,
                            ResourcePatternType = ResourcePatternType.Literal
                        },
                        Entry = new AccessControlEntry
                        {
                            Principal = "User:ANONYMOUS",
                            Host =  "*",
                            Operation = AclOperation.Delete,
                            PermissionType = AclPermissionType.Allow
                        }
                    },
                };
                createAclsException = adminClient.CreateAclsAsync(topicACLs, createAclsOptions).Exception;
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DescribeTopics");
        }
    }
}
