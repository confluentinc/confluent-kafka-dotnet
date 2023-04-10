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
        ///     Test functionality of AdminClient.DescribeCLuster and
        ///     We test three cases:
        ///     1. Without creating Acls and without includeClusterAuthorizedOperations .
        ///     2. Without creating Acls and with includeClusterAuthorizedOperations.
        ///     3. After creating Acls and without includeClusterAuthorizedOperations.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_DescribeCluster(string bootstrapServers)
        {
            LogToFile("start AdminClient_DescribeCluster");

            // Create an AdminClient here - we need it throughout the test.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig {
                BootstrapServers = bootstrapServers }).Build())
            {
                var listOptionsWithTimeout = new Admin.ListConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(30) };
                var describeOptionsWithTimeout = new Admin.DescribeClusterOptions() { RequestTimeout = TimeSpan.FromSeconds(30) , IncludeClusterAuthorizedOperations = false};
                var describeOptionsWithAuthOps = new Admin.DescribeClusterOptions() { RequestTimeout = TimeSpan.FromSeconds(30) , IncludeClusterAuthorizedOperations = true};

                var descResult = adminClient.DescribeClusterAsync(describeOptionsWithTimeout).Result;
                var clusterDesc = descResult.clusterDescription;

                Assert.NotEmpty(clusterDesc.Nodes);
                Assert.Empty(clusterDesc.AuthorizedOperations);

                descResult = adminClient.DescribeClusterAsync(describeOptionsWithAuthOps).Result;
                clusterDesc = descResult.clusterDescription;
                Assert.NotEmpty(clusterDesc.AuthorizedOperations);
                var initialCount = clusterDesc.AuthorizedOperations.Count;
                
                LogToFile($"{initialCount} initial");

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
                            Principal = "User:ANONYMOUS",
                            Host =  "*",
                            Operation = AclOperation.Alter,
                            PermissionType = AclPermissionType.Allow
                        }
                    },
                };

                var clusterACLFilter = new List<AclBindingFilter>{
                    new AclBindingFilter()
                    {
                        PatternFilter = new ResourcePatternFilter
                        {
                            Type = ResourceType.Broker,
                            Name = "kafka-cluster",
                            ResourcePatternType = ResourcePatternType.Literal
                        },
                        EntryFilter = new AccessControlEntryFilter
                        {
                            Principal = "User:ANONYMOUS",
                            Host = "*",
                            Operation = AclOperation.Alter,
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
                var createAclsException = adminClient.CreateAclsAsync(clusterACLs, createAclsOptions).Exception;
                
                descResult = adminClient.DescribeClusterAsync(describeOptionsWithAuthOps).Result;
                clusterDesc = descResult.clusterDescription;

                Assert.NotEmpty(clusterDesc.Nodes);
                Assert.NotEmpty(clusterDesc.AuthorizedOperations);

                var finalCount = clusterDesc.AuthorizedOperations.Count;
                LogToFile($"{finalCount} final");
                var resultDeleteAcls = adminClient.DeleteAclsAsync(
                    clusterACLFilter, deleteAclsOptions
                ).Result;

                Assert.Single(resultDeleteAcls);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_DescribeCluster");
        }
    }
}
