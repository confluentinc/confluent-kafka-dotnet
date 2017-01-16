using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class GroupInfoTests
    {
        [Fact]
        public void Constuctor()
        {
            var bmd = new BrokerMetadata(1, "host", 42);
            var members = new List<GroupMemberInfo>();
            var gi = new GroupInfo(bmd, "mygroup", new Error(ErrorCode.NO_ERROR), "mystate", "myprotocoltype", "myprotocol", members);
            Assert.Equal(gi.Broker, bmd);
            Assert.Equal(gi.Group, "mygroup");
            Assert.Equal(gi.Error, new Error(ErrorCode.NO_ERROR));
            Assert.Equal(gi.State, "mystate");
            Assert.Equal(gi.ProtocolType, "myprotocoltype");
            Assert.Equal(gi.Protocol, "myprotocol");
            Assert.Same(gi.Members, members);
        }
    }
}
