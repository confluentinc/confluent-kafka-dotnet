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
        ///     Test functionality of AdminClient.CreateAcls.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async void AdminClient_CreateAcls(string bootstrapServers)
        {
            LogToFile("start AdminClient_CreateAcls");

            var topicName = Guid.NewGuid().ToString();
            var groupName = Guid.NewGuid().ToString();
            var maxDuration = TimeSpan.FromSeconds(30);
            var noError = new Error(ErrorCode.NoError, "", false);
            var unknownError = new Error(ErrorCode.Unknown, "Unknown broker error", false);

            var newACLs = new List<AclBinding>()
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

            var invalidACLs = new List<AclBinding>()
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

            var validAndInvalidACLs =  new List<AclBinding>()
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

            CreateAclsOptions createAclsOptions = new CreateAclsOptions()
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
                        Assert.Equal(noError, resultCreateAcls[j].Error);
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
                Assert.Equal(invalidACLs.Count(), createAclsException.Results.Count());
                for (int j = 0; j < invalidACLs.Count(); j++)
                {
                    Assert.Equal(unknownError, createAclsException.Results[j].Error);
                }
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
                Assert.Equal(validAndInvalidACLs.Count(), createAclsException.Results.Count());
                Assert.Equal(unknownError, createAclsException.Results[0].Error);
                Assert.Equal(noError, createAclsException.Results[1].Error);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end AdminClient_CreateAcls");
        }
    }
}
