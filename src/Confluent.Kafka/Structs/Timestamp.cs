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
