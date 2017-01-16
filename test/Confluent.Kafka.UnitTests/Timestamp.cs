using System;
using Xunit;


namespace Confluent.Kafka.Tests
{
    public class TimestampTests
    {
        [Fact]
        public void Constuctor()
        {
            var ts = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            Assert.Equal(ts.DateTime, new DateTime(2010, 3, 4));
            Assert.Equal(ts.Type, TimestampType.CreateTime);
        }

        [Fact]
        public void Equality()
        {
            var ts1 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            var ts2 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.CreateTime);
            var ts3 = new Timestamp(new DateTime(2011, 3, 4), TimestampType.CreateTime);
            var ts4 = new Timestamp(new DateTime(2010, 3, 4), TimestampType.LogAppendTime);

            Assert.Equal(ts1, ts2);
            Assert.True(ts1.Equals(ts2));
            Assert.True(ts1 == ts2);
            Assert.False(ts1 != ts2);

            Assert.NotEqual(ts1, ts3);
            Assert.False(ts1.Equals(ts3));
            Assert.False(ts1 == ts3);
            Assert.True(ts1 != ts3);

            Assert.NotEqual(ts1, ts4);
            Assert.False(ts1.Equals(ts4));
            Assert.False(ts1 == ts4);
            Assert.True(ts1 != ts4);
        }

        [Fact]
        public void Conversion()
        {
            // check is to millisecond accuracy.
            var ts = new DateTime(2012, 5, 6, 12, 4, 3, 220, DateTimeKind.Utc);
            var unixTime = Timestamp.DateTimeToUnixTimestampMs(ts);
            var ts2 = Timestamp.UnixTimestampMsToDateTime(unixTime);
            Assert.Equal(ts, ts2);
            Assert.Equal(ts2.Kind, DateTimeKind.Unspecified);
        }
    }
}
