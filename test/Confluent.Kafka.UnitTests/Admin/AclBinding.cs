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
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class AclBindingTests
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
            var acl3 = new AclBinding {
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
            var acl4 = new AclBinding
            {
                Pattern = new ResourcePattern
                {
                    Type = ResourceType.Unknown,
                    Name = null,
                    ResourcePatternType = ResourcePatternType.Unknown
                },
                Entry = new AccessControlEntry
                {
                    Principal = null,
                    Host = null,
                    Operation = AclOperation.Unknown,
                    PermissionType = AclPermissionType.Unknown
                }
            };
            var aclF1 = new AclBindingFilter
            {
                PatternFilter = new ResourcePatternFilter
                {
                    Type = ResourceType.Topic,
                    Name = "a1",
                    ResourcePatternType = ResourcePatternType.Literal
                },
                EntryFilter = new AccessControlEntryFilter
                {
                    Principal = "p1",
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };
            var aclF2 = new AclBindingFilter
            {
                PatternFilter = new ResourcePatternFilter
                {
                    Type = ResourceType.Topic,
                    Name = "a1",
                    ResourcePatternType = ResourcePatternType.Literal
                },
                EntryFilter = new AccessControlEntryFilter
                {
                    Principal = "p1",
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };

            Assert.NotEqual(acl1, acl2);
            Assert.NotEqual(acl2, acl1);
            Assert.Equal(acl2, acl3);
            Assert.True(acl1 != acl2);
            Assert.True(acl2 != acl1);
            Assert.True(acl2 == acl3);
            Assert.NotEqual(acl4, acl1);
            Assert.NotEqual(acl4, acl2);
            Assert.Equal(acl4, acl4);
            Assert.True(acl4 != acl1);
            Assert.True(acl4 != acl2);
            Assert.False(acl4 == acl1);
            Assert.False(acl4 == acl2);
            Assert.Equal(aclF1, aclF2);
            Assert.True(aclF1 == aclF2);
            Assert.False(aclF1 != aclF2);
        }
    
        [Fact]
        public void ToStringTest()
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
                    Name = null,
                    ResourcePatternType = ResourcePatternType.Literal
                },
                Entry = new AccessControlEntry
                {
                    Principal = null,
                    Host = "\"h1\"",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };
            var aclFilter1 = new AclBindingFilter
            {
                PatternFilter = new ResourcePatternFilter
                {
                    Type = ResourceType.Topic,
                    Name = null,
                    ResourcePatternType = ResourcePatternType.Literal
                },
                EntryFilter = new AccessControlEntryFilter
                {
                    Principal = null,
                    Host = "h1",
                    Operation = AclOperation.All,
                    PermissionType = AclPermissionType.Allow
                }
            };

            var expectedString1 = @"{""Pattern"": {""Type"": ""Topic"", ""Name"": null, ""ResourcePatternType"": ""Literal""}, ""Entry"": {""Principal"": null, ""Host"": ""h1"", ""Operation"": ""All"", ""PermissionType"": ""Allow""}}";
            var expectedString2 = @"{""Pattern"": {""Type"": ""Topic"", ""Name"": null, ""ResourcePatternType"": ""Literal""}, ""Entry"": {""Principal"": null, ""Host"": ""\""h1\"""", ""Operation"": ""All"", ""PermissionType"": ""Allow""}}";

            Assert.Equal(expectedString1, acl1.ToString());
            Assert.NotEqual(acl1.ToString(), aclFilter1.ToString());
            Assert.NotEqual(acl1.ToString(), acl2.ToString());
            Assert.Equal(expectedString2, acl2.ToString());
        }
    }
}
