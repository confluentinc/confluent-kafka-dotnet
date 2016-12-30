using Xunit;


namespace Confluent.Kafka.Tests
{
    public class ErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var e = new Error(ErrorCode._BAD_COMPRESSION, "bad compression");
            Assert.Equal(e.Code, ErrorCode._BAD_COMPRESSION);
            Assert.Equal(e.Message, "bad compression");
        }

        [Fact]
        public void Equality()
        {
            var e1 = new Error(ErrorCode._ALL_BROKERS_DOWN, null);
            var e2 = new Error(ErrorCode._ALL_BROKERS_DOWN, "all brokers down");
            var e3 = new Error(ErrorCode._IN_PROGRESS, null);

            Assert.Equal(e1, e2);
            Assert.True(e1.Equals(e2));
            Assert.True(e1 == e2);
            Assert.False(e1 != e2);

            Assert.NotEqual(e1, e3);
            Assert.False(e1.Equals(e3));
            Assert.False(e1 == e3);
            Assert.True(e1 != e3);
        }

        [Fact]
        public void ToStringTest()
        {
            var e = new Error(ErrorCode._ASSIGN_PARTITIONS, "assign partitions");
            Assert.True(e.ToString().Contains(((int)ErrorCode._ASSIGN_PARTITIONS).ToString()));
            Assert.True(e.ToString().Contains("assign partitions"));
        }
    }
}
