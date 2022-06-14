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
    public class CreateAclsErrorTests
    {

        private List<AclBinding> copyAclBindings(List<AclBinding> original)
        {
            return original.Select((aclBinding) => {
                return aclBinding.Clone();
            }).ToList();
        }

        [Fact]
        public async void Errors()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:666" }).Build())
            {

                var testAclBindings = new List<AclBinding>
                {
                    new AclBinding()
                    {
                        Type = ResourceType.Topic,
                        Name = "mytopic",
                        ResourcePatternType = ResourcePatternType.Literal,
                        Principal = "User:myuser",
                        Host = "*",
                        Operation = AclOperation.All,
                        PermissionType = AclPermissionType.Allow
                    },
                };

                // null aclBindings
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                    adminClient.CreateAclsAsync(null)
                );

                // empty aclBindings
                await Assert.ThrowsAsync<ArgumentException>(() =>
                    adminClient.CreateAclsAsync(new List<AclBinding>())
                );

                CreateAclsOptions options = new CreateAclsOptions()
                {
                    RequestTimeout = TimeSpan.FromMilliseconds(200)
                };

                // Correct input, fail with timeout
                var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                    adminClient.CreateAclsAsync(testAclBindings, options)
                );
                Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);

                // Invalid ACL bindings
                var invalidTests = new List<List<AclBinding>>
                {
                    copyAclBindings(testAclBindings),
                    copyAclBindings(testAclBindings),
                };
                invalidTests[0][0].Type = ResourceType.Unknown;
                invalidTests[1][0].Type = ResourceType.Any;
                var expectedError = "Invalid resource type";
                foreach (List<AclBinding> invalidTest in invalidTests)
                {
                    var exInvalidTest = await Assert.ThrowsAsync<KafkaException>(() =>
                         adminClient.CreateAclsAsync(invalidTest)
                    );
                    Assert.EndsWith(expectedError, exInvalidTest.Message);
                }

                var suffixes = new List<string>()
                {
                    "Invalid resource pattern type",
                    "Invalid resource pattern type",
                    "Invalid resource pattern type",
                    "Invalid operation",
                    "Invalid operation",
                    "Invalid permission type",
                    "Invalid permission type",
                    "Invalid resource name",
                    "Invalid principal",
                    "Invalid host",
                };
                invalidTests = suffixes.Select((suffix) => {
                    return copyAclBindings(testAclBindings);
                }).ToList();
                invalidTests[0][0].ResourcePatternType = ResourcePatternType.Unknown;
                invalidTests[1][0].ResourcePatternType = ResourcePatternType.Match;
                invalidTests[2][0].ResourcePatternType = ResourcePatternType.Any;
                invalidTests[3][0].Operation = AclOperation.Unknown;
                invalidTests[4][0].Operation = AclOperation.Any;
                invalidTests[5][0].PermissionType = AclPermissionType.Unknown;
                invalidTests[6][0].PermissionType = AclPermissionType.Any;
                invalidTests[7][0].Name = null;
                invalidTests[8][0].Principal = null;
                invalidTests[9][0].Host = null;

                var i = 0;
                foreach (List<AclBinding> invalidTest in invalidTests)
                {
                    var exInvalidTest = await Assert.ThrowsAsync<KafkaException>(() =>
                         adminClient.CreateAclsAsync(invalidTest)
                    );
                    Assert.EndsWith(suffixes[i], exInvalidTest.Message);
                    i++;
                }
            }
        }
    }
}