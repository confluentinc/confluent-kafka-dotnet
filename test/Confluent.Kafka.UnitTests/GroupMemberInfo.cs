using Xunit;


namespace Confluent.Kafka.Tests
{
    public class GroupMemberInfoTests
    {
        [Fact]
        public void Constuctor()
        {
            var memberMetadata = new byte[0];
            var memberAssignment = new byte[0];
            var gmi = new GroupMemberInfo("mymember", "myclient", "clienthost", memberMetadata, memberAssignment);
            Assert.Equal(gmi.MemberId, "mymember");
            Assert.Equal(gmi.ClientId, "myclient");
            Assert.Equal(gmi.ClientHost, "clienthost");
            Assert.Same(gmi.MemberMetadata, memberMetadata);
            Assert.Same(gmi.MemberAssignment, memberAssignment);
        }
    }
}
