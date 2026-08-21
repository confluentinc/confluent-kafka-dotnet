// Copyright 2026 Confluent Inc.
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
using Google.Protobuf.WellKnownTypes;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Conversion helpers backing the <c>timestamp</c> overloads. The CEL surface uses
    ///     the built-in timestamp type (<see cref="CelTypeLabels.TimestampName" />); this
    ///     client backs it with a Protobuf <see cref="Timestamp" />, which cel.net's
    ///     <c>TimestampT</c> wraps. Counterpart of Java's <c>TimestampUtils</c>.
    /// </summary>
    internal static class TimestampUtils
    {
        private static long FloorDiv(long x, long y)
        {
            long q = x / y;
            if (x % y != 0 && (x ^ y) < 0)
            {
                q--;
            }

            return q;
        }

        private static long FloorMod(long x, long y) => x - FloorDiv(x, y) * y;

        public static Timestamp FromEpochMillis(long ms) =>
            new Timestamp { Seconds = FloorDiv(ms, 1_000L), Nanos = (int)(FloorMod(ms, 1_000L) * 1_000_000L) };

        public static Timestamp FromEpochMicros(long us) =>
            new Timestamp { Seconds = FloorDiv(us, 1_000_000L), Nanos = (int)(FloorMod(us, 1_000_000L) * 1_000L) };

        public static Timestamp FromEpochNanos(long ns) =>
            new Timestamp { Seconds = FloorDiv(ns, 1_000_000_000L), Nanos = (int)FloorMod(ns, 1_000_000_000L) };

        public static Timestamp FromEpochSeconds(long s) => new Timestamp { Seconds = s, Nanos = 0 };

        /// <summary>
        ///     Construct from an epoch numeric value at a Flink-style decimal precision:
        ///     0 seconds, 3 millis, 6 micros, 9 nanos. Precisions outside that set are rejected
        ///     rather than generalized to "any p means 10^-p": with the unit a number rather than
        ///     a name, that check is the only thing between a typo and a silently wrong instant.
        /// </summary>
        public static Timestamp FromEpochPrecision(long value, long precision)
        {
            switch (precision)
            {
                case 0: return FromEpochSeconds(value);
                case 3: return FromEpochMillis(value);
                case 6: return FromEpochMicros(value);
                case 9: return FromEpochNanos(value);
                default:
                    throw new ArgumentException(
                        $"timestamp: unknown precision {precision}; expected 0 (seconds), " +
                        "3 (millis), 6 (micros) or 9 (nanos)");
            }
        }

    }
}
