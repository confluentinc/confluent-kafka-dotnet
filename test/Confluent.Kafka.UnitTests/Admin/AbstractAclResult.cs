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
    public class AbstractAclResultTests
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

            var res1 = new DeleteAclsResult {};
            var res2 = new DeleteAclsResult {};
            var res3 = new DeleteAclsResult
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
            };
            var res4 = new DeleteAclsResult
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
                AclBindings = new List<AclBinding>
                {
                    acl1,
                    acl2
                }
            };
            var res5 = new DeleteAclsResult
            {
                Error = res4.Error,
                AclBindings = res4.AclBindings
            };
            var res6 = new DeleteAclsResult
            {
                Error = new Error(ErrorCode.NoError, "Other message", false),
                AclBindings = new List<AclBinding>
                {
                    acl1.Clone(),
                    acl2.Clone()
                }
            };
            var res7 = new DeleteAclsResult
            {
                Error = new Error(ErrorCode.NoError, "Success", true),
                AclBindings = new List<AclBinding>
                {
                    acl1.Clone(),
                    acl2.Clone()
                }
            };

            Assert.Equal(res1, res2);
            Assert.True(res1 == res2);
            Assert.NotEqual(res1, res3);
            Assert.False(res1 == res3);
            Assert.NotEqual(res3, res4);
            Assert.True(res3 != res4);
            Assert.Equal(res4, res5);
            Assert.True(res4 == res5);
            Assert.Equal(res4, res6);
            Assert.True(res4 == res6);
            Assert.NotEqual(res6, res7);
            Assert.True(res6 != res7);
        }
    }
}
