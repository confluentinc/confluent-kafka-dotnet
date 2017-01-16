using System;

namespace Confluent.Kafka
{
    public static class TimeSpanExtensions
    {
        internal static int TotalMillisecondsAsInt(this TimeSpan timespan)
        {
            int millisecondsTimespan;
            checked { millisecondsTimespan = (int)timespan.TotalMilliseconds; }
            return millisecondsTimespan;
        }
    }
}
