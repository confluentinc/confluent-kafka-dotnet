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
        ///     Initializes a new instance of the Timestamp structure.
        /// </summary>
        /// <param name="dateTime">
        ///     The timestamp.
        /// </param>
        /// <param name="type">
        ///     The type of the timestamp.
        /// </param>
        public Timestamp(DateTime dateTime, TimestampType type)
        {
            Type = type;
            DateTime = dateTime;
        }

        /// <summary>
        ///     Gets the timestamp type.
        /// </summary>
        public TimestampType Type { get; }

        /// <summary>
        ///     Gets the timestamp value.
        /// </summary>
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

        public override int GetHashCode()
            => Type.GetHashCode()*251 + DateTime.GetHashCode();  // x by prime number is quick and gives decent distribution.

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
        /// </returns>
        public static long DateTimeToUnixTimestampMs(DateTime dateTime)
        {
            checked
            {
                return (long)(dateTime.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc)).TotalMilliseconds;
            }
        }

        /// <summary>
        ///     Convert a milliseconds unix timestamp to a DateTime value.
        /// </summary>
        /// <param name="unixMillisecondsTimestamp">
        ///     The milliseconds unix timestamp to convert.
        /// </param>
        /// <returns>
        ///     The DateTime value associated with <paramref name="unixMillisecondsTimestamp"/>
        /// </returns>
        public static DateTime UnixTimestampMsToDateTime(long unixMillisecondsTimestamp)
            => new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc)
                + TimeSpan.FromMilliseconds(unixMillisecondsTimestamp);
    }
}
