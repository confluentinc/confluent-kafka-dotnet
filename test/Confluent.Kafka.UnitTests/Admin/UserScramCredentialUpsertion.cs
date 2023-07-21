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

using Confluent.Kafka.Admin;
using Xunit;
using System.Text;


namespace Confluent.Kafka.UnitTests
{
    public class UserScramCredentialUpsertionTests
    {
        [Fact]
        public void StringRepresentation()
        {
            // Upsertion
            var upsertion = new UserScramCredentialUpsertion
            {
                User = "test",
                ScramCredentialInfo = new ScramCredentialInfo()
                {
                    Mechanism = ScramMechanism.ScramSha256,
                    Iterations = 10000
                },
                Password = Encoding.UTF8.GetBytes("password"),
                Salt = Encoding.UTF8.GetBytes("salt")
            };
            Assert.Equal(
                @"{""User"": ""test"", ""ScramCredentialInfo"": " +
                @"{""Mechanism"": ""ScramSha256"", ""Iterations"": 10000}}",
                upsertion.ToString());
                
            // Empty salt
            upsertion = new UserScramCredentialUpsertion
            {
                User = "test1",
                ScramCredentialInfo = new ScramCredentialInfo()
                {
                    Mechanism = ScramMechanism.ScramSha512,
                    Iterations = 5000
                },
                Password = Encoding.UTF8.GetBytes("password"),
                Salt = null
            };
            Assert.Equal(
                @"{""User"": ""test1"", ""ScramCredentialInfo"": " +
                @"{""Mechanism"": ""ScramSha512"", ""Iterations"": 5000}}",
                upsertion.ToString());
        }
    }
}
