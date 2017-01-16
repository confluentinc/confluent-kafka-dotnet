using Xunit;


namespace Confluent.Kafka.Tests
{
    public class LogMessageTests
    {
        [Fact]
        public void Constuctor()
        {
            var lm = new LogMessage("myname", 42, "myfacility", "mymessage");
            Assert.Equal(lm.Name, "myname");
            Assert.Equal(lm.Level, 42);
            Assert.Equal(lm.Facility, "myfacility");
            Assert.Equal(lm.Message, "mymessage");
        }
    }
}
