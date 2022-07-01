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
        ///     Test functionality of AdminClient ACL operations.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async void AdminClient_AclOperations(string bootstrapServers)
        {
            LogToFile("start AdminClient_AclOperations");

            var topicName = Guid.NewGuid().ToString();
            var groupName = Guid.NewGuid().ToString();
            var maxDuration = TimeSpan.FromSeconds(30);
            var noError = new Error(ErrorCode.NoError, "", false);
            var unknownError = new Error(ErrorCode.Unknown, "Unknown broker error", false);
            var noErrorCreateResult = new CreateAclResult
            {
                Error = noError
            };
            var unknownErrorCreateResult = new CreateAclResult
            {
                Error = unknownError
            };

            var newACLs = new List<AclBinding>
            {
                    new AclBinding()
                    {
                        Type = ResourceType.Topic,
                        Name = topicName,
                        ResourcePatternType = ResourcePatternType.Literal,
                        Principal = "User:test-user-1",
                        Host = "*",
                        Operation = AclOperation.Read,
                        PermissionType = AclPermissionType.Allow,
                    },
                    new AclBinding()
                    {
                        Type = ResourceType.Topic,
                        Name = topicName,
                        ResourcePatternType = ResourcePatternType.Prefixed,
                        Principal = "User:test-user-2",
                        Host = "*",
                        Operation = AclOperation.Write,
                        PermissionType = AclPermissionType.Deny,
                    },
                    new AclBinding()
                    {
                        Type = ResourceType.Group,
                        Name = groupName,
                        ResourcePatternType = ResourcePatternType.Prefixed,
                        Principal = "User:test-user-2",
                        Host = "some-host",
                        Operation = AclOperation.All,
                        PermissionType = AclPermissionType.Allow,
                    },
            };

            var invalidACLs = new List<AclBinding>
            {
                new AclBinding()
                {
                    Type = ResourceType.Topic,
                    Name = topicName,
                    ResourcePatternType = ResourcePatternType.Literal,
                    // Principal must be in the form "{principalType}:{principalName}"
                    // Broker returns ErrUnknown in this case
                    Principal = "wrong-principal",
                    Host = "*",
                    Operation = AclOperation.Read,
                    PermissionType = AclPermissionType.Allow,
                },
            };

            var validAndInvalidACLs =  new List<AclBinding>
            {
                new AclBinding()
                {
                    Type = ResourceType.Topic,
                    Name = topicName,
                    ResourcePatternType = ResourcePatternType.Literal,
                    // Principal must be in the form "{principalType}:{principalName}"
                    // Broker returns ErrUnknown in this case
                    Principal = "wrong-principal",
                    Host = "*",
                    Operation = AclOperation.Read,
                    PermissionType = AclPermissionType.Allow,
                },
                new AclBinding()
                {
                    Type = ResourceType.Group,
                    Name = groupName,
                    ResourcePatternType = ResourcePatternType.Prefixed,
                    Principal = "User:test-user-2",
                    Host = "some-host",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow,
                },
            };

            var aclBindingFilters = new List<AclBindingFilter>
            {
                new AclBindingFilter()
                {
                    Type = ResourceType.Any,
                    ResourcePatternType = ResourcePatternType.Any,
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any,
                },
                new AclBindingFilter()
                {
                    Type = ResourceType.Any,
                    ResourcePatternType = ResourcePatternType.Prefixed,
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any,
                },
                new AclBindingFilter()
                {
                    Type = ResourceType.Topic,
                    ResourcePatternType = ResourcePatternType.Any,
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any,
                },
                new AclBindingFilter()
                {
                    Type = ResourceType.Group,
                    ResourcePatternType = ResourcePatternType.Any,
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any,
                },
            };

            CreateAclsOptions createAclsOptions = new CreateAclsOptions
            {
                RequestTimeout = maxDuration
            };
            DeleteAclsOptions deleteAclsOptions = new DeleteAclsOptions
            {
                RequestTimeout = maxDuration
            };

            //  - construction of admin client from configuration.
            //  - creation of multiple ACL.
            //  - CreateAcls should be idempotent
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                for (int i = 0; i < 2; i++)
                {
                    var resultCreateAcls = await adminClient.CreateAclsAsync(
                        newACLs,
                        createAclsOptions
                    );
                    Assert.Equal(newACLs.Count(), resultCreateAcls.Count());
                    for (int j = 0; j < newACLs.Count(); j++)
                    {
                        Assert.Equal(noErrorCreateResult, resultCreateAcls[j]);
                    }
                }
            }

            //  - construction of admin client from a producer handle
            //  - CreateACLs with server side validation errors
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                var createAclsException = await Assert.ThrowsAsync<CreateAclsException>(() =>
                    adminClient.CreateAclsAsync(
                        invalidACLs,
                        createAclsOptions
                    )
                );
                Assert.Equal(new CreateAclsException(
                    new List<CreateAclResult> { unknownErrorCreateResult }
                ), createAclsException);
            }

            //  - construction of admin client from a producer handle
            //  - CreateACLs with errors and succeeded items
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                var createAclsException = await Assert.ThrowsAsync<CreateAclsException>(() =>
                    adminClient.CreateAclsAsync(
                        validAndInvalidACLs,
                        createAclsOptions
                    )
                );
                Assert.Equal(new CreateAclsException(
                    new List<CreateAclResult> { unknownErrorCreateResult, noErrorCreateResult }
                ), createAclsException);
            }

            //  - construction of admin client from configuration.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                //  DeleteAcls with ResourcePatternType Prefixed
                var resultDeleteAcls = await adminClient.DeleteAclsAsync(
                    new List<AclBindingFilter>
                    {
                        aclBindingFilters[1]
                    },
                    deleteAclsOptions
                );
                Assert.Single(resultDeleteAcls);
                Assert.Equal(2, resultDeleteAcls[0].AclBindings.Count());
                Assert.Equal(new DeleteAclsResult
                {
                    Error = noError,
                    AclBindings = new List<AclBinding>
                    {
                        newACLs[1],
                        newACLs[2]
                    }
                }, resultDeleteAcls[0]);

                //  DeleteAcls with ResourceType Topic and Group
                resultDeleteAcls = await adminClient.DeleteAclsAsync(
                    new List<AclBindingFilter>
                    {
                        aclBindingFilters[2],
                        aclBindingFilters[3],
                    },
                    deleteAclsOptions
                );
                Assert.Equal(2, resultDeleteAcls.Count());
                Assert.Equal(new DeleteAclsResult
                {
                    Error = noError,
                    AclBindings = new List<AclBinding>
                    {
                        newACLs[0]
                    }
                }, resultDeleteAcls[0]);
                Assert.Equal(new DeleteAclsResult
                {
                    Error = noError,
                    AclBindings = new List<AclBinding>()
                }, resultDeleteAcls[1]);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end AdminClient_AclOperations");
        }
    }
}
