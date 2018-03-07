// Copyright 2016-2017 Confluent Inc.
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


namespace Confluent.Kafka.UnitTests
{
    public class GroupMemberInfoTests
    {
        [Fact]
        public void Constuctor()
        {
            var memberMetadata = new byte[0];
            var memberAssignment = new byte[0];
            var gmi = new GroupMemberInfo("mymember", "myclient", "clienthost", memberMetadata, memberAssignment);
            Assert.Equal("mymember", gmi.MemberId);
            Assert.Equal("myclient", gmi.ClientId);
            Assert.Equal("clienthost", gmi.ClientHost);
            Assert.Same(memberMetadata, gmi.MemberMetadata);
            Assert.Same(memberAssignment, gmi.MemberAssignment);
        }
    }
}
