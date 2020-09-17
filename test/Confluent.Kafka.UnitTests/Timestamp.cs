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

using System;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class TimestampTests
    {
        [Fact]
        public void ConstructorDefault()
        {
            var ts = new Timestamp();
            Assert.Equal(ts, Timestamp.Default);
        }

        [Fact]
        public void ConstuctorUnix()
        {
            var ts1 = new Timestamp(123456789, TimestampType.CreateTime);
            var ts2 = new Timestamp(-123456789, TimestampType.LogAppendTime);

            Assert.Equal(123456789, ts1.UnixTimestampMs);
            Assert.Equal(-123456789, ts2.UnixTimestampMs);

            Assert.Equal(TimestampType.CreateTime, ts1.Type);
            Assert.Equal(TimestampType.LogAppendTime, ts2.Type);
        }
#pragma warning disable CS0618 // Type or member is obsolete
        [Fact]
        public void ConstructorDateTime()
        {
            var dt1 = new DateTime(2008, 1, 1, 0, 0, 0, DateTimeKind.Local);
            var dt2 = new DateTime(2008, 1, 1, 0, 0, 0, DateTimeKind.Utc);


            var ts1 = new Timestamp(dt1, TimestampType.CreateTime);
            var ts2 = new Timestamp(dt2, TimestampType.LogAppendTime);

            var ts3 = new Timestamp(dt1);
            var ts4 = new Timestamp(dt2);

            Assert.Equal(ts1, ts3);
            Assert.Equal(ts2.UnixTimestampMs, ts4.UnixTimestampMs);

            var utcOffset = TimeZoneInfo.Local.GetUtcOffset(dt1);
            var utcOffsetMs = utcOffset.TotalMilliseconds;

            Assert.Equal(ts1.UnixTimestampMs + utcOffsetMs, ts2.UnixTimestampMs);
            Assert.Equal(ts3.UnixTimestampMs + utcOffsetMs, ts4.UnixTimestampMs);
        }


        [Fact]
        public void ConstructorDateTimeOffset()
        {
            var dt1 = new DateTime(2008, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var dto1 = new DateTimeOffset(new DateTime(2008, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), TimeSpan.FromHours(2));
            var dto2 = new DateTimeOffset(new DateTime(2008, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimeSpan.FromSeconds(0));

            var ts1 = new Timestamp(dto1);
            var ts2 = new Timestamp(dto2);
            var ts3 = new Timestamp(dt1);

            Assert.Equal(ts1.UnixTimestampMs + TimeSpan.FromHours(2).TotalMilliseconds, ts2.UnixTimestampMs);
            Assert.Equal(ts3.UnixTimestampMs, ts2.UnixTimestampMs);
        }

#pragma warning restore CS0618 // Type or member is obsolete
        [Fact]
        public void Equality()
        {
            var ts1 = new Timestamp(1, TimestampType.CreateTime);
            var ts2 = new Timestamp(1, TimestampType.CreateTime);
            var ts3 = new Timestamp(2, TimestampType.CreateTime);
            var ts4 = new Timestamp(1, TimestampType.LogAppendTime);

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
            var ts = new DateTimeOffset(2012, 5, 6, 12, 4, 3, 220, TimeSpan.Zero);
            var unixTime = Timestamp.DateTimeOffsetToUnixTimestampMs(ts);
            var ts2 = Timestamp.UnixTimestampMsToDateTimeOffset(unixTime);
            Assert.Equal(1336305843220, unixTime);
            Assert.Equal(ts, ts2);
        }

        [Fact]
        public void UnixTimeEpoch()
        {
#pragma warning disable CS0618 // Type or member is obsolete
            Assert.Equal(0, Timestamp.DateTimeToUnixTimestampMs(Timestamp.UnixTimeEpoch));
#pragma warning restore CS0618 // Type or member is obsolete
        }

        [Fact]
        public void UnixTimeEpochDateTimeOffset()
        {
            Assert.Equal(0, Timestamp.DateTimeOffsetToUnixTimestampMs(Timestamp.UnixTimeEpochDateTimeOffset));
        }

        [Fact]
        public void DateTimeProperties()
        {
            var ts = new Timestamp(1, TimestampType.CreateTime);
#pragma warning disable CS0618 // Type or member is obsolete
            Assert.Equal(1, (ts.UtcDateTime - Timestamp.UnixTimeEpoch).TotalMilliseconds);
#pragma warning restore CS0618 // Type or member is obsolete
        }

        [Fact]
        public void DateTimeOffsetProperties()
        {
            var ts = new Timestamp(1, TimestampType.CreateTime);
            Assert.Equal(1, (ts.DateTimeOffset - Timestamp.UnixTimeEpochDateTimeOffset).TotalMilliseconds);
        }

        [Fact]
        public void Rounding()
        {
            // check is to millisecond accuracy, rounding down the value
            
            var dateTimeAfterEpoch = new DateTimeOffset(2012, 5, 6, 12, 4, 3, 220, TimeSpan.Zero);
            var dateTimeBeforeEpoch = new DateTimeOffset(1950, 5, 6, 12, 4, 3, 220, TimeSpan.Zero);

            foreach (var datetime in new[] { dateTimeAfterEpoch, dateTimeBeforeEpoch })
            {
                var unixTime1 = Timestamp.DateTimeOffsetToUnixTimestampMs(datetime.AddTicks(1));
                var unixTime2 = Timestamp.DateTimeOffsetToUnixTimestampMs(datetime.AddTicks(TimeSpan.TicksPerMillisecond - 1));
                var unixTime3 = Timestamp.DateTimeOffsetToUnixTimestampMs(datetime.AddTicks(TimeSpan.TicksPerMillisecond));
                var unixTime4 = Timestamp.DateTimeOffsetToUnixTimestampMs(datetime.AddTicks(-1));

                var expectedUnixTime = Timestamp.DateTimeOffsetToUnixTimestampMs(datetime);
                
                Assert.Equal(expectedUnixTime, unixTime1);
                Assert.Equal(expectedUnixTime, unixTime2);
                Assert.Equal(expectedUnixTime + 1, unixTime3);
                Assert.Equal(expectedUnixTime - 1, unixTime4);
            }
        }
    }
}
