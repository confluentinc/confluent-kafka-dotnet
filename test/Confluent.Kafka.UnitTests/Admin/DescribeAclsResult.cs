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
    public class DescribeAclsResultTests
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

            var res1 = new DescribeAclsResult {};
            var res2 = new DescribeAclsResult {};
            var res3 = new DescribeAclsResult
            {
                AclBindings = new List<AclBinding>
                {
                    acl1,
                    acl2
                }
            };
            var res4 = new DescribeAclsResult
            {
                AclBindings = res3.AclBindings
            };
            var res5 = new DescribeAclsResult
            {
                AclBindings = new List<AclBinding>
                {
                    acl1.Clone(),
                    acl2.Clone()
                }
            };

            Assert.Equal(res1, res2);
            Assert.True(res1 == res2);
            Assert.Equal(res3, res4);
            Assert.True(res3 == res4);
            Assert.Equal(res3, res5);
            Assert.True(res3 == res5);
        }
    }
}
