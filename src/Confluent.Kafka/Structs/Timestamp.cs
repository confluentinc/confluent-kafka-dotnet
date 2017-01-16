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

        public TimestampType Type { get; }
        public DateTime DateTime { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is Timestamp))
            {
                return false;
            }

            var ts = (Timestamp)obj;
            return ts.Type == Type && ts.DateTime == DateTime;
        }

        // x by prime number is quick and gives decent distribution.
        public override int GetHashCode()
            => Type.GetHashCode()*251 + DateTime.GetHashCode();

        public static bool operator ==(Timestamp a, Timestamp b)
            => a.Equals(b);

        public static bool operator !=(Timestamp a, Timestamp b)
            => !(a == b);

        public static long DateTimeToUnixTimestampMs(DateTime dateTime)
        {
            checked
            {
                return (long)(dateTime.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc)).TotalMilliseconds;
            }
        }

        public static DateTime UnixTimestampMsToDateTime(long timestamp)
            => new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Unspecified)
                + TimeSpan.FromMilliseconds(timestamp);
    }
}
