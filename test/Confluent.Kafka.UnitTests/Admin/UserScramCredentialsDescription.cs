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

using System.Collections.Generic;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class UserScramCredentialsDescriptionTests
    {
        [Fact]
        public void StringRepresentation()
        {
            // Success case
            var description = new UserScramCredentialsDescription
            {
                User = "test",
                ScramCredentialInfos = new List<ScramCredentialInfo> 
                {
                    new ScramCredentialInfo()
                    {
                        Mechanism = ScramMechanism.ScramSha256,
                        Iterations = 10000
                    },
                    new ScramCredentialInfo()
                    {
                        Mechanism = ScramMechanism.ScramSha512,
                        Iterations = 5000
                    }
                },
                Error = ErrorCode.NoError
            };
            Assert.Equal(
                @"{""User"": ""test"", ""ScramCredentialInfos"": " +
                @"[{""Mechanism"": ""ScramSha256"", ""Iterations"": 10000}, " + 
                @"{""Mechanism"": ""ScramSha512"", ""Iterations"": 5000}], ""Error"": ""Success""}",
                description.ToString());

            // Resource not found error
            description = new UserScramCredentialsDescription
            {
                User = "test",
                ScramCredentialInfos = new List<ScramCredentialInfo> 
                {
                },
                Error = ErrorCode.ResourceNotFound
            };
            Assert.Equal(
                @"{""User"": ""test"", ""ScramCredentialInfos"": [], " +
                @"""Error"": ""Broker: Request illegally referred to " + 
                @"resource that does not exist""}",
                description.ToString());
        }
    }
}
