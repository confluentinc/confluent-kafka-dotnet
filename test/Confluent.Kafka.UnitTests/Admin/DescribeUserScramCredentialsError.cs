// Copyright 2023 Confluent Inc.
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


namespace Confluent.Kafka.UnitTests
{
    public class DescribeUserErrorTests
    {
        private readonly ICollection<ICollection<string>> correctUsers = new List<ICollection<string>>
        {
            new List<string> {
                "user1",
                "user2"
            }.AsReadOnly(),
            new List<string> {
            }.AsReadOnly()
        };

        private readonly ICollection<ICollection<string>> emptyUsers = new List<ICollection<string>>
        {
            new List<string> {
                "",
                "user1",
            }.AsReadOnly(),
            new List<string> {
                ""
            }.AsReadOnly()
        };

        private readonly DescribeUserScramCredentialsOptions options = new DescribeUserScramCredentialsOptions
        {
            RequestTimeout = TimeSpan.FromMilliseconds(200)
        };

        [Fact]
        public async void LocalTimeout()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                // Correct input, fail with timeout
                // try multiple times with the same AdminClient
                foreach (var users in correctUsers)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                                        adminClient.DescribeUserScramCredentialsAsync(users, options)
                                    );
                    Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
                }
            }
        }
        
        [Fact]
        public async void EmptyUsers()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                // Some users are an empty string
                // try multiple times with the same AdminClient
                foreach (var users in emptyUsers)
                {
                    var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
                                        adminClient.DescribeUserScramCredentialsAsync(users, options)
                                    );
                    Assert.Equal("Cannot have a null or empty user", ex.Message);
                }
            }
        }
    }
}
