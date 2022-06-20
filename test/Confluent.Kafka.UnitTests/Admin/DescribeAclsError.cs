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
    public class DescribeAclsErrorTests
    {

        [Fact]
        public async void Errors()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {

                var testAclBindingFilters = new List<AclBindingFilter>
                {
                    new AclBindingFilter()
                    {
                        Type = ResourceType.Any,
                        ResourcePatternType = ResourcePatternType.Match,
                        Operation = AclOperation.Any,
                        PermissionType = AclPermissionType.Any
                    },
                    new AclBindingFilter()
                    {
                        Type = ResourceType.Any,
                        ResourcePatternType = ResourcePatternType.Match,
                        Operation = AclOperation.Any,
                        PermissionType = AclPermissionType.Any
                    },
                };

                // null aclBindingFilters
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.DescribeAclsAsync(null)
                );

                var options = new DescribeAclsOptions
                {
                    RequestTimeout = TimeSpan.FromMilliseconds(200)
                };

                // Correct input, fail with timeout
                foreach (AclBindingFilter aclBindingFilter in testAclBindingFilters)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                                        adminClient.DescribeAclsAsync(aclBindingFilter, options)
                                    );
                    Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
                }

                // Invalid ACL binding filters
                var suffixes = new List<string>()
                {
                    "Invalid resource type",
                    "Invalid resource pattern type",
                    "Invalid operation",
                    "Invalid permission type",
                };
                var invalidTests = suffixes.Select((suffix) => {
                    return testAclBindingFilters[0].Clone();
                }).ToList();
                invalidTests[0].Type = ResourceType.Unknown;
                invalidTests[1].ResourcePatternType = ResourcePatternType.Unknown;
                invalidTests[2].Operation = AclOperation.Unknown;
                invalidTests[3].PermissionType = AclPermissionType.Unknown;

                int i = 0;
                foreach (AclBindingFilter invalidTest in invalidTests)
                {
                    var exInvalidTest = await Assert.ThrowsAsync<KafkaException>(() =>
                         adminClient.DescribeAclsAsync(invalidTest)
                    );
                    Assert.EndsWith(suffixes[i], exInvalidTest.Message);
                    i++;
                }
            }
        }
    }
}
