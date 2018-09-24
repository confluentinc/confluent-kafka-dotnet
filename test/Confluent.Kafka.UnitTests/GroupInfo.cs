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

using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class GroupInfoTests
    {
        [Fact]
        public void Constuctor()
        {
            var bmd = new BrokerMetadata(1, "host", 42);
            var members = new List<GroupMemberInfo>();
            var gi = new GroupInfo(bmd, "mygroup", new Error(ErrorCode.NoError), "mystate", "myprotocoltype", "myprotocol", members);
            Assert.Equal(bmd, gi.Broker);
            Assert.Equal("mygroup", gi.Group);
            Assert.Equal(new Error(ErrorCode.NoError), gi.Error);
            Assert.Equal("mystate", gi.State);
            Assert.Equal("myprotocoltype", gi.ProtocolType);
            Assert.Equal("myprotocol", gi.Protocol);
            Assert.Same(members, gi.Members);
        }
    }
}
