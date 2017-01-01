using System;

namespace Confluent.Kafka
{
    public static class TimeSpanExtensions
    {
        public static int TotalMillisecondsAsInt(this TimeSpan timespan)
        {
            int millisecondsTimespan;
            checked { millisecondsTimespan = (int)timespan.TotalMilliseconds; }
            return millisecondsTimespan;
        }
    }
}
