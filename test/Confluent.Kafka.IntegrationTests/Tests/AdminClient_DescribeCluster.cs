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
        ///     Test functionality of AdminClient.DescribeCluster and
        ///     We test three cases:
        ///     1. Without creating ACLs and without includeAuthorizedOperations .
        ///     2. Without creating ACLs and with includeAuthorizedOperations.
        ///     3. After creating ACLs with the user matched by the ACL.
        /// </summary>
        [Theory, MemberData(nameof(SaslPlainKafkaParameters))]
        public async void AdminClient_DescribeCluster(string bootstrapServers,
            string admin, string adminSecret, string user, string userSecret)
        {
            LogToFile("start AdminClient_DescribeCluster");

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = admin,
                SaslPassword = adminSecret,
                BootstrapServers = bootstrapServers
            }).Build())
            {
                var listOptionsWithTimeout = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };
                var describeOptionsWithTimeout = new Admin.DescribeClusterOptions() { RequestTimeout = TimeSpan.FromSeconds(30) , IncludeAuthorizedOperations = false};
                var describeOptionsWithAuthOps = new Admin.DescribeClusterOptions() { RequestTimeout = TimeSpan.FromSeconds(30) , IncludeAuthorizedOperations = true};

                var descResult = await adminClient.DescribeClusterAsync(describeOptionsWithTimeout);

                Assert.NotEmpty(descResult.Nodes);
                Assert.Null(descResult.AuthorizedOperations);

                descResult = await adminClient.DescribeClusterAsync(describeOptionsWithAuthOps);
                Assert.Equal(7, descResult.AuthorizedOperations.Count);
                
                var clusterACLs =  new List<AclBinding>
                {
                    new AclBinding()
                    {
                        Pattern = new ResourcePattern
                        {
                            Type = ResourceType.Broker,
                            Name = "kafka-cluster",
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

                var clusterACLFilters = clusterACLs.Select(acl =>
                    acl.ToFilter()).ToList();

                var createAclsOptions = new CreateAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                await adminClient.CreateAclsAsync(clusterACLs, createAclsOptions);
                
                using (var adminClientUser = new AdminClientBuilder(new AdminClientConfig
                {
                    SecurityProtocol = SecurityProtocol.SaslPlaintext,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = user,
                    SaslPassword = userSecret,
                    BootstrapServers = bootstrapServers
                }).Build())
                {
                     descResult = await adminClientUser.DescribeClusterAsync(describeOptionsWithAuthOps);
                }

                Assert.NotEmpty(descResult.Nodes);
                Assert.NotEmpty(descResult.AuthorizedOperations);

                var finalCount = descResult.AuthorizedOperations.Count;
                Assert.Equal(new List<AclOperation>()
                {
                    AclOperation.Alter,
                    AclOperation.Describe,
                }, descResult.AuthorizedOperations);

                var deleteAclsOptions = new DeleteAclsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(30)
                };
                var resultDeleteAcls = await adminClient.DeleteAclsAsync(
                    clusterACLFilters, deleteAclsOptions
                );

                Assert.Single(resultDeleteAcls);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DescribeCluster");
        }
    }
}
