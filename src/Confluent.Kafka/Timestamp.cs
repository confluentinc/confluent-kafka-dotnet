// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates a Kafka timestamp and its type.
    /// </summary>
    public struct Timestamp
    {
        /// <summary>
        ///     Unix epoch as UTC DateTime. Unix time is defined as 
        ///     the number of seconds past this UTC time, excluding leap seconds
        /// </summary>
        public static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private const long UnixEpochMilliseconds = 62135596800000; // = UnixTimeEpoch.TotalMiliseconds

        /// <summary>
        ///     Initializes a new instance of the Timestamp structure.
        /// </summary>
        /// <param name="unixTimestampMs">
        ///     The unix millisecond timestamp.
        /// </param>
        /// <param name="type">
        ///     The type of the timestamp.
        /// </param>
        public Timestamp(long unixTimestampMs, TimestampType type)
        {
            Type = type;
            UnixTimestampMs = unixTimestampMs;
        }

        /// <summary>
        ///     Initializes a new instance of the Timestamp structure.
        ///     Prefer the (long, TimestampType) overload if possible.
        /// </summary>
        /// <param name="dateTime">
        ///     The datetime, which will be converted to
        ///     the nearest unix millisecond timestamp rounded down
        ///     via <see cref="DateTimeToUnixTimestampMs"/>.
        /// </param>
        /// <param name="type">
        ///     The type of the timestamp.
        /// </param>
        /// <remarks>
        ///     The <see cref="DateTime"/> property may differ from this datetime
        ///     (it will be UTC with a millisecond precision)
        /// </remarks>
        [Obsolete("Use the (long, TimestampType) constructor instead, this will be removed in future release")]
        public Timestamp(DateTime dateTime, TimestampType type)
        {
            Type = type;
            UnixTimestampMs = DateTimeToUnixTimestampMs(dateTime);
        }

        /// <summary>
        ///     Gets the timestamp type.
        /// </summary>
        public TimestampType Type { get; }

        /// <summary>
        ///     Get the Unix millisecond timestamp.
        /// </summary>
        public long UnixTimestampMs { get; }

        /// <summary>
        ///     Gets the Utc DateTime corresponding to the <see cref="UnixTimestampMs"/>.
        /// </summary>
        [Obsolete("Prefer DateTimeOffset to avoid DateTimeKind issue, this may be removed in a future release")]
        public DateTime DateTime
            => DateTimeOffset.UtcDateTime;

        /// <summary>
        ///     Gets the DateTimeOffset corresponding to the <see cref="UnixTimestampMs"/>.
        /// </summary>
        public DateTimeOffset DateTimeOffset
            => new DateTimeOffset(UnixTimestampMsToDateTime(UnixTimestampMs));

        public override bool Equals(object obj)
        {
            if (!(obj is Timestamp))
            {
                return false;
            }

            var ts = (Timestamp)obj;
            return ts.Type == Type && ts.UnixTimestampMs == UnixTimestampMs;
        }

        public override int GetHashCode()
            => Type.GetHashCode() * 251 + UnixTimestampMs.GetHashCode();  // x by prime number is quick and gives decent distribution.

        public static bool operator ==(Timestamp a, Timestamp b)
            => a.Equals(b);

        public static bool operator !=(Timestamp a, Timestamp b)
            => !(a == b);

        /// <summary>
        ///     Convert a DateTime instance to a milliseconds unix timestamp.
        ///     Note: <paramref name="dateTime"/> is first converted to UTC if it is not already.
        /// </summary>
        /// <param name="dateTime">
        ///     The DateTime value to convert.
        /// </param>
        /// <returns>
        ///     The milliseconds unix timestamp corresponding to <paramref name="dateTime"/>
        ///     rounded down to the previous millisecond.
        /// </returns>
        public static long DateTimeToUnixTimestampMs(DateTime dateTime)
            => dateTime.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond - UnixEpochMilliseconds;

        /// <summary>
        ///     Convert a milliseconds unix timestamp to a DateTime value.
        /// </summary>
        /// <param name="unixMillisecondsTimestamp">
        ///     The milliseconds unix timestamp to convert.
        /// </param>
        /// <returns>
        ///     The DateTime value associated with <paramref name="unixMillisecondsTimestamp"/> with Utc Kind.
        /// </returns>
        public static DateTime UnixTimestampMsToDateTime(long unixMillisecondsTimestamp)
            => UnixTimeEpoch + TimeSpan.FromMilliseconds(unixMillisecondsTimestamp);
    }
}
