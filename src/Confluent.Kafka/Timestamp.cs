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
    public struct Timestamp : IEquatable<Timestamp>
    {
        private const long RD_KAFKA_NO_TIMESTAMP = 0;

        /// <summary>
        ///     A read-only field representing an unspecified timestamp.
        /// </summary>
        public static Timestamp Default
        {
            get { return new Timestamp(RD_KAFKA_NO_TIMESTAMP, TimestampType.NotAvailable); }
        }

        /// <summary>
        ///     Unix epoch as a UTC DateTime. Unix time is defined as 
        ///     the number of seconds past this UTC time, excluding 
        ///     leap seconds.
        /// </summary>
        public static readonly DateTime UnixTimeEpoch 
            = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private const long UnixTimeEpochMilliseconds 
            = 62135596800000; // = UnixTimeEpoch.TotalMiliseconds


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
        ///     Note: <paramref name="dateTime"/> is first converted to UTC 
        ///     if it is not already.
        /// </summary>
        /// <param name="dateTime">
        ///     The DateTime value corresponding to the timestamp.
        /// </param>
        /// <param name="type">
        ///     The type of the timestamp.
        /// </param>
        public Timestamp(DateTime dateTime, TimestampType type)
        {
            Type = type;
            UnixTimestampMs = DateTimeToUnixTimestampMs(dateTime);
        }

        /// <summary>
        ///     Initializes a new instance of the Timestamp structure.
        ///     Note: <paramref name="dateTime" /> is first converted
        ///     to UTC if it is not already and TimestampType is set
        ///     to CreateTime.
        /// </summary>
        /// <param name="dateTime">
        ///     The DateTime value corresponding to the timestamp.
        /// </param>
        public Timestamp(DateTime dateTime)
            : this(dateTime, TimestampType.CreateTime) 
        {}

        /// <summary>
        ///     Initializes a new instance of the Timestamp structure.
        ///     Note: TimestampType is set to CreateTime.
        /// </summary>
        /// <param name="dateTimeOffset">
        ///     The DateTimeOffset value corresponding to the timestamp.
        /// </param>
        public Timestamp(DateTimeOffset dateTimeOffset)
            : this(dateTimeOffset.UtcDateTime, TimestampType.CreateTime) 
        {}

        /// <summary>
        ///     Gets the timestamp type.
        /// </summary>
        public TimestampType Type { get; }

        /// <summary>
        ///     Get the Unix millisecond timestamp.
        /// </summary>
        public long UnixTimestampMs { get; }

        /// <summary>
        ///     Gets the UTC DateTime corresponding to the <see cref="UnixTimestampMs"/>.
        /// </summary>
        public DateTime UtcDateTime
            => UnixTimestampMsToDateTime(UnixTimestampMs);

        /// <summary>
        ///     Determines whether two Timestamps have the same value.
        /// </summary>
        /// <param name="obj">
        ///     Determines whether this instance and a specified object, 
        ///     which must also be a Timestamp object, have the same value.
        /// </param>
        /// <returns>
        ///     true if obj is a Timestamp and its value is the same as 
        ///     this instance; otherwise, false. If obj is null, the method 
        ///     returns false.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj is Timestamp ts)
            {
                return Equals(ts);
            }

            return false;
        }

        /// <summary>
        ///     Determines whether two Timestamps have the same value.
        /// </summary>
        /// <param name="other">
        ///     The timestamp to test.
        /// </param>
        /// <returns>
        ///     true if other has the same value. false otherwise.
        /// </returns>
        public bool Equals(Timestamp other)
            => other.Type == Type && other.UnixTimestampMs == UnixTimestampMs;

        /// <summary>
        ///     Returns the hashcode for this Timestamp.
        /// </summary>
        /// <returns>
        ///     A 32-bit signed integer hash code.
        /// </returns>
        public override int GetHashCode()
            => Type.GetHashCode() * 251 + UnixTimestampMs.GetHashCode();  // x by prime number is quick and gives decent distribution.

        /// <summary>
        ///     Determines whether two specified Timestamps have the same value.
        /// </summary>
        /// <param name="a">
        ///     The first Timestamp to compare.
        /// </param>
        /// <param name="b">
        ///     The second Timestamp to compare
        /// </param>
        /// <returns>
        ///     true if the value of a is the same as the value of b; otherwise, false.
        /// </returns>
        public static bool operator ==(Timestamp a, Timestamp b)
            => a.Equals(b);

        /// <summary>
        ///     Determines whether two specified Timestamps have different values.
        /// </summary>
        /// <param name="a">
        ///     The first Timestamp to compare.
        /// </param>
        /// <param name="b">
        ///     The second Timestamp to compare
        /// </param>
        /// <returns>
        ///     true if the value of a is different from the value of b; otherwise, false.
        /// </returns>
        public static bool operator !=(Timestamp a, Timestamp b)
            => !(a == b);

        /// <summary>
        ///     Convert a DateTime instance to a milliseconds unix timestamp.
        ///     Note: <paramref name="dateTime"/> is first converted to UTC 
        ///     if it is not already.
        /// </summary>
        /// <param name="dateTime">
        ///     The DateTime value to convert.
        /// </param>
        /// <returns>
        ///     The milliseconds unix timestamp corresponding to <paramref name="dateTime"/>
        ///     rounded down to the previous millisecond.
        /// </returns>
        public static long DateTimeToUnixTimestampMs(DateTime dateTime)
            => dateTime.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond - UnixTimeEpochMilliseconds;

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
