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
using System.Text;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.UnitTests
{
    public class AlterUserScramCredentialsErrorTests
    {
        private readonly ICollection<ICollection<UserScramCredentialAlteration>> correctAlterations =
        new List<ICollection<UserScramCredentialAlteration>>
        {
            new List<UserScramCredentialAlteration>
            {
                new UserScramCredentialUpsertion
                {
                    User = "user1",
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 10,
                    },
                    Password = Encoding.UTF8.GetBytes("password1"),
                    Salt = Encoding.UTF8.GetBytes("salt1"),
                },
                new UserScramCredentialUpsertion
                {
                    User = "user1",
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha512,
                        Iterations = 20,
                    },
                    Password = Encoding.UTF8.GetBytes("password2"),
                },
            }.AsReadOnly(),
            new List<UserScramCredentialAlteration>
            {
                new UserScramCredentialUpsertion
                {
                    User = "user2",
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 10,
                    },
                    Password = Encoding.UTF8.GetBytes("password3"),
                },
                new UserScramCredentialDeletion
                {
                    User = "user2",
                    Mechanism = ScramMechanism.ScramSha512,
                },
            }.AsReadOnly(),
            new List<UserScramCredentialAlteration>
            {
                new UserScramCredentialUpsertion
                {
                    User = "user2",
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 10,
                    },
                    Password = Encoding.UTF8.GetBytes("password3"),
                },
                new UserScramCredentialDeletion
                {
                    User = "user2",
                    Mechanism = ScramMechanism.ScramSha256,
                },
            }.AsReadOnly()
        };

        private readonly ICollection<ICollection<UserScramCredentialAlteration>> nullAlterations =
        new List<ICollection<UserScramCredentialAlteration>>
        {
            new List<UserScramCredentialAlteration>
            {
                new UserScramCredentialUpsertion
                {
                    User = "user1",
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 10,
                    },
                    Password = Encoding.UTF8.GetBytes("password1"),
                    Salt = Encoding.UTF8.GetBytes("salt1"),
                },
                null,
            }.AsReadOnly(),
            new List<UserScramCredentialAlteration>
            {
                null,
                new UserScramCredentialDeletion
                {
                    User = "user2",
                    Mechanism = ScramMechanism.ScramSha512,
                },
            }.AsReadOnly(),
            new List<UserScramCredentialAlteration>
            {
                null,
            }.AsReadOnly(),
        };
        
        private class NewUserScramCredentialAlteration : UserScramCredentialAlteration
        {
            public ScramMechanism OtherMechanism {get; set;}
        }

        private readonly AlterUserScramCredentialsOptions options = new AlterUserScramCredentialsOptions
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
                foreach (var alterations in correctAlterations)
                {
                    var ex = await Assert.ThrowsAsync<KafkaException>(() =>
                                        adminClient.AlterUserScramCredentialsAsync(alterations, options)
                                    );
                    Assert.Equal("Failed while waiting for controller: Local: Timed out", ex.Message);
                }
            }
        }
        
        [Fact]
        public async void NullAlterations()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                // Null alterations.
                // try multiple times with the same AdminClient
                foreach (var alterations in nullAlterations)
                {
                    var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
                                        adminClient.AlterUserScramCredentialsAsync(alterations, options)
                                    );
                    Assert.Equal("Cannot have a null alteration", ex.Message);
                }
            }
        }
        
        [Fact]
        public async void InvalidSubclass()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build())
            {
                // Invalid subclass
                var alterations = new List<UserScramCredentialAlteration>
                {
                    new NewUserScramCredentialAlteration
                    {
                        User = "user3",
                        OtherMechanism = ScramMechanism.ScramSha256
                    },
                }.AsReadOnly();
                var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
                                    adminClient.AlterUserScramCredentialsAsync(alterations, options)
                               );
                Assert.Equal("Every alteration must be either a UserScramCredentialDeletion " + 
                             "or UserScramCredentialUpsertion", ex.Message);
            }
        }
    }
}
