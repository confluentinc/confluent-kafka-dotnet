using System;


namespace Confluent.Kafka
{
    public struct Timestamp
    {
        public Timestamp(DateTime dateTime, TimestampType type)
        {
            Type = type;
            DateTime = dateTime;
        }

        public TimestampType Type { get; set; }
        public DateTime DateTime { get; set; }

        public static long DateTimeToUnixTimestampMs(DateTime dateTime)
        {
            return (long)(dateTime.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc)).TotalMilliseconds;
        }

        public static DateTime UnixTimestampMsToDateTime(long timestamp)
        {
            // TODO: what kind of time is it better to return here? UTC or Local?
            return
                new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Local)
                + TimeSpan.FromMilliseconds(timestamp);
        }
    }
}
