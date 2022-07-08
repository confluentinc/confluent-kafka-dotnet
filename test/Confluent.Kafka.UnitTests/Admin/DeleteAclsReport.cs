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
    public class DeleteAclsReportTests
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

            var rep1 = new DeleteAclsReport {};
            var rep2 = new DeleteAclsReport {};
            var rep3 = new DeleteAclsReport
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
            };
            var rep4 = new DeleteAclsReport
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
                AclBindings = new List<AclBinding>
                {
                    acl1,
                    acl2
                }
            };
            var rep5 = new DeleteAclsReport
            {
                Error = rep4.Error,
                AclBindings = rep4.AclBindings
            };
            var rep6 = new DeleteAclsReport
            {
                Error = new Error(ErrorCode.NoError, "Other message", false),
                AclBindings = new List<AclBinding>
                {
                    acl1.Clone(),
                    acl2.Clone()
                }
            };
            var rep7 = new DeleteAclsReport
            {
                Error = new Error(ErrorCode.NoError, "Success", true),
                AclBindings = new List<AclBinding>
                {
                    acl1.Clone(),
                    acl2.Clone()
                }
            };

            Assert.Equal(rep1, rep2);
            Assert.True(rep1 == rep2);
            Assert.NotEqual(rep1, rep3);
            Assert.False(rep1 == rep3);
            Assert.NotEqual(rep3, rep4);
            Assert.True(rep3 != rep4);
            Assert.Equal(rep4, rep5);
            Assert.True(rep4 == rep5);
            Assert.Equal(rep4, rep6);
            Assert.True(rep4 == rep6);
            Assert.NotEqual(rep6, rep7);
            Assert.True(rep6 != rep7);
        }
    }
}
