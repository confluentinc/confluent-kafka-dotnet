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

#pragma warning disable xUnit1026

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Test functionality of AdminClient.DescribeUserScramCredentials and AdminClient.AlterUserScramCredentials.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public async void AdminClient_UserScramCredentials(string bootstrapServers)
        {
            LogToFile("start AdminClient_UserScramCredentials");
            var timeout = TimeSpan.FromSeconds(10);
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var users = new List<string>
                {
                    "non-existing-user"
                };
                var describeOptions =
                    new DescribeUserScramCredentialsOptions() { RequestTimeout = timeout };
                var alterOptions =
                    new AlterUserScramCredentialsOptions() { RequestTimeout = timeout };
                List<UserScramCredentialsDescription> descriptions;
                
                try
                {
                    await adminClient.DescribeUserScramCredentialsAsync(users, describeOptions);
                    Assert.True(false, "Describe request shouldn't succeed");
                }
                catch (DescribeUserScramCredentialsException e)
                {
                    descriptions = e.Results.UserScramCredentialsDescriptions;
                    foreach (var description in descriptions)
                    {
                        Assert.Equal(users[0], description.User);
                        Assert.Equal(ErrorCode.ResourceNotFound,description.Error.Code);
                    }
                }

                var upsertions = new List<UserScramCredentialAlteration>();
                var upsertion = new UserScramCredentialUpsertion()
                {
                    User = users[0],
                    ScramCredentialInfo = new ScramCredentialInfo
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 15000,
                    },
                    Password = Encoding.UTF8.GetBytes("Password"),
                    Salt = Encoding.UTF8.GetBytes("Salt")
                };
                upsertions.Add(upsertion);
                await adminClient.AlterUserScramCredentialsAsync(upsertions, alterOptions);

                var descResult = await adminClient.DescribeUserScramCredentialsAsync(users, describeOptions);
                foreach (var description in descResult.UserScramCredentialsDescriptions)
                {
                    Assert.Equal(users[0], description.User);
                    Assert.Equal(ErrorCode.NoError, description.Error.Code);
                    foreach(var credentialinfo in description.ScramCredentialInfos){
                        Assert.Equal(15000,credentialinfo.Iterations);
                        Assert.Equal(ScramMechanism.ScramSha256, credentialinfo.Mechanism);
                    }
                }

                var deletions = new List<UserScramCredentialAlteration>();
                var deletion = new UserScramCredentialDeletion()
                {
                    User = "non-existing-user",
                    Mechanism = ScramMechanism.ScramSha256
                };
                deletions.Add(deletion);
                await adminClient.AlterUserScramCredentialsAsync(deletions, alterOptions);

                try
                {
                    await adminClient.DescribeUserScramCredentialsAsync(users, describeOptions);
                    Assert.True(false, "Describe request shouldn't succeed");
                }
                catch (DescribeUserScramCredentialsException e)
                {
                    descriptions = e.Results.UserScramCredentialsDescriptions;
                    foreach (var description in descriptions)
                    {
                        Assert.Equal(users[0], description.User);
                        Assert.Equal(ErrorCode.ResourceNotFound,description.Error.Code);
                    }
                }
            }
            Assert.Equal(0, Library.HandleCount);
            LogToFile("end AdminClient_UserScramCredentials");
        }
    }
}
