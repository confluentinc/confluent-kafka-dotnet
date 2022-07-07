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

using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class CreateAclReportTests
    {
        [Fact]
        public void Equality()
        {
            var res1 = new CreateAclReport {};
            var res2 = new CreateAclReport {};
            var res3 = new CreateAclReport
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
            };
            var res4 = new CreateAclReport
            {
                Error = new Error(ErrorCode.NoError, "Success", false),
            };
            var res5 = new CreateAclReport
            {
                Error = res4.Error,
            };
            var res6 = new CreateAclReport
            {
                Error = new Error(ErrorCode.NoError, "Other message", false),
            };
            var res7 = new CreateAclReport
            {
                Error = new Error(ErrorCode.NoError, "Success", true),
            };

            Assert.Equal(res1, res2);
            Assert.True(res1 == res2);
            Assert.NotEqual(res1, res3);
            Assert.False(res1 == res3);
            Assert.Equal(res3, res4);
            Assert.True(res3 == res4);
            Assert.Equal(res4, res5);
            Assert.True(res4 == res5);
            Assert.Equal(res4, res6);
            Assert.True(res4 == res6);
            Assert.NotEqual(res6, res7);
            Assert.True(res6 != res7);
        }
    }
}
