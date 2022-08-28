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

using Xunit;
using System;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using System.Linq;


namespace Confluent.Kafka.UnitTests
{
    public class DeleteAclsErrorTests
    {
        private readonly IList<AclBindingFilter> testAclBindingFilters = new List<AclBindingFilter>
        {
            new AclBindingFilter()
            {
                PatternFilter = new ResourcePatternFilter
                {
                    Type = ResourceType.Any,
                    ResourcePatternType = ResourcePatternType.Match
                },
                EntryFilter = new AccessControlEntryFilter
                {
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any
                }
            },
            new AclBindingFilter()
            {
                PatternFilter = new ResourcePatternFilter
                {
                    Type = ResourceType.Any,
                    ResourcePatternType = ResourcePatternType.Any
                },
                EntryFilter = new AccessControlEntryFilter
                {
                    Operation = AclOperation.Any,
                    PermissionType = AclPermissionType.Any
                }
            },
        }.AsReadOnly();

        private readonly DeleteAclsOptions options = new DeleteAclsOptions
        {
            RequestTimeout = TimeSpan.FromMilliseconds(200)
        };

        private static IList<AclBindingFilter> CopyAclBindingFilters(IList<AclBindingFilter> original)
        {
            return original.Select((aclBinding) => {
                return aclBinding.Clone();
            }).ToList().AsReadOnly();
        }

        [Fact]
        public async void NullAclBindingFilters()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.DeleteAclsAsync(null)
                );
            }
        }

        [Fact]
        public async void EmptyAclBindingFilters()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                await Assert.ThrowsAsync<ArgumentException>(() =>
                    adminClient.DeleteAclsAsync(new List<AclBindingFilter>())
                );
            }
        }

        [Fact]
        public async void NullResourcePattern()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.DeleteAclsAsync(new List<AclBindingFilter>
                    {
                        new AclBindingFilter
                        {
                            EntryFilter = testAclBindingFilters[0].EntryFilter
                        }
                    })
                );
            }
        }

        [Fact]
        public async void NullAccessControlEntry()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.DeleteAclsAsync(new List<AclBindingFilter>
                    {
                        new AclBindingFilter
                        {
                            PatternFilter = testAclBindingFilters[0].PatternFilter
                        }
                    })
                );
            }
        }

        [Fact]
        public async void LocalTimeout()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                // Correct input, fail with timeout
                var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.DeleteAclsAsync(testAclBindingFilters, options)
                );
                Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
            }
        }

        [Fact]
        public async void InvalidAclBindingFilters()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                var suffixes = new List<string>()
                {
                    "Invalid resource type",
                    "Invalid resource pattern type",
                    "Invalid operation",
                    "Invalid permission type",
                };
                var invalidTests = suffixes.Select((suffix) => {
                    return CopyAclBindingFilters(testAclBindingFilters);
                }).ToList();
                invalidTests[0][0].PatternFilter.Type = ResourceType.Unknown;
                invalidTests[1][0].PatternFilter.ResourcePatternType = ResourcePatternType.Unknown;
                invalidTests[2][0].EntryFilter.Operation = AclOperation.Unknown;
                invalidTests[3][0].EntryFilter.PermissionType = AclPermissionType.Unknown;

                var i = 0;
                foreach (IList<AclBindingFilter> invalidTest in invalidTests)
                {
                    var exInvalidTest = await Assert.ThrowsAsync<KafkaException>(() =>
                         adminClient.DeleteAclsAsync(invalidTest)
                    );
                    Assert.EndsWith(suffixes[i], exInvalidTest.Message);
                    ++i;
                }
            }
        }
    }
}
