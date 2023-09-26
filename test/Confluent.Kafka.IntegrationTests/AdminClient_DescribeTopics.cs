// Copyright 2023 Confluent Inc.
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
        [Theory, MemberData(nameof(SaslPlainKafkaParameters))]
        public async void AdminClient_DescribeTopics(string bootstrapServers,
            string admin, string adminSecret, string user, string userSecret)
        {
            LogToFile("start AdminClient_DescribeTopics");

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = admin,
                SaslPassword = adminSecret
            }).Build())
            {
                var describeOptionsWithTimeout = new Admin.DescribeTopicsOptions()
                { 
                    RequestTimeout = TimeSpan.FromSeconds(30), 
                    IncludeAuthorizedOperations = false
                };
                var describeOptionsWithAuthOps = new Admin.DescribeTopicsOptions()
                { 
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
                    var descResult = await adminClient.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames(topicList),
                        describeOptionsWithTimeout);
                }
                catch (DescribeTopicsException ex)
                {
                    var resCount = ex.Results.TopicDescriptions.Count;
                    Assert.Equal(2, resCount);
                    Assert.Null(ex.Results.TopicDescriptions[0].AuthorizedOperations);
                    Assert.Null(ex.Results.TopicDescriptions[1].AuthorizedOperations);
                    Assert.True(ex.Results.TopicDescriptions[0].Error.IsError);
                    Assert.False(ex.Results.TopicDescriptions[1].Error.IsError);
                }

                var topicListAuthOps = 
                TopicCollection.OfTopicNames(
                    new List<string>
                    {
                        singlePartitionTopic
                    }
                );
                var descResWithAuthOps = await adminClient.DescribeTopicsAsync(
                    topicListAuthOps,
                    describeOptionsWithAuthOps);
                Assert.NotEmpty(descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations);
                Assert.Equal(8, descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations.Count);

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
                            Principal = $"User:{user}",
                            Host =  "*",
                            Operation = AclOperation.Read,
                            PermissionType = AclPermissionType.Allow
                        }
                    },
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
                            Principal = $"User:{user}",
                            Host =  "*",
                            Operation = AclOperation.Alter,
                            PermissionType = AclPermissionType.Allow
                        }
                    },
                };

                var createAclsOptions = new CreateAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                await adminClient.CreateAclsAsync(topicACLs, createAclsOptions);

                using (var adminClientUser = new AdminClientBuilder(new AdminClientConfig
                {
                    SecurityProtocol = SecurityProtocol.SaslPlaintext,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = user,
                    SaslPassword = userSecret,
                    BootstrapServers = bootstrapServers
                }).Build())
                {
                     descResWithAuthOps = await adminClientUser.DescribeTopicsAsync(topicListAuthOps,
                        describeOptionsWithAuthOps);
                }

                var descResAuthOps =
                    descResWithAuthOps.TopicDescriptions[0].AuthorizedOperations;
                Assert.Equal(
                    new List<AclOperation>()
                    {
                        AclOperation.Read,
                        AclOperation.Alter,
                        AclOperation.Describe, // implicit because of Read.
                    },
                    descResAuthOps
                );

                var deleteAclsOptions = new DeleteAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                var deleteTopicACLs = topicACLs.Select((acl) =>
                    acl.ToFilter()
                ).ToList();
                await adminClient.DeleteAclsAsync(deleteTopicACLs, deleteAclsOptions);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end AdminClient_DescribeTopics");
        }
    }
}
