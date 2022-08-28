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

using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class DeleteAclsExceptionTests
    {
        [Fact]
        public void Equality()
        {
            var acl1 = new AclBinding
            {
                Pattern = new ResourcePattern
                {
                    Type = ResourceType.Topic,
                    Name = null,
                    ResourcePatternType = ResourcePatternType.Literal
                },
                Entry = new AccessControlEntry
                {
                    Principal = null,
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };
            var acl2 = new AclBinding
            {
                Pattern = new ResourcePattern
                {
                    Type = ResourceType.Topic,
                    Name = "a1",
                    ResourcePatternType = ResourcePatternType.Literal
                },
                Entry = new AccessControlEntry
                {
                    Principal = "p1",
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };
            var acl3 = new AclBinding
            {
                Pattern = new ResourcePattern
                {
                    Type = ResourceType.Topic,
                    Name = "a2",
                    ResourcePatternType = ResourcePatternType.Literal
                },
                Entry = new AccessControlEntry
                {
                    Principal = "p1",
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };

            var ex1 = new DeleteAclsException(new List<DeleteAclsReport>());
            var ex2 = new DeleteAclsException(new List<DeleteAclsReport>
            {
                new DeleteAclsReport()
                {
                    Error = new Error(ErrorCode.NoError, "Success", false),
                    AclBindings = new List<AclBinding>
                    {
                        acl1,
                        acl2
                    }
                }
            });
            var ex3 = new DeleteAclsException(new List<DeleteAclsReport>
            {
                new DeleteAclsReport()
                {
                    Error = new Error(ErrorCode.NoError, "Other message", false),
                    AclBindings = new List<AclBinding>
                    {
                        acl2.Clone(),
                        acl1.Clone(),
                        acl1.Clone(),
                    }
                }
            });
            var ex4 = new DeleteAclsException(new List<DeleteAclsReport>
            {
                new DeleteAclsReport()
                {
                    Error = new Error(ErrorCode.NoError, "Other message", false),
                    AclBindings = new List<AclBinding>
                    {
                        acl1.Clone(),
                        acl2.Clone(),
                        acl3.Clone(),
                    }
                }
            });

            Assert.NotEqual(ex1, ex2);
            Assert.True(ex1 != ex2);
            Assert.Equal(ex2, ex3);
            Assert.True(ex2 == ex3);
            Assert.NotEqual(ex3, ex4);
            Assert.True(ex3 != ex4);
        }
    }
}
