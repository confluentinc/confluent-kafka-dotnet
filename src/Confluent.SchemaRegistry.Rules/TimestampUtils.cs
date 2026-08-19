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
using System.Globalization;
using Google.Protobuf.WellKnownTypes;
using NodaTime;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Conversion helpers backing the <c>timestamp.of</c> overloads. The CEL surface uses
    ///     the built-in timestamp type (<see cref="CelTypeLabels.TimestampName" />); this
    ///     client backs it with a Protobuf <see cref="Timestamp" />, which cel.net's
    ///     <c>TimestampT</c> wraps. Counterpart of Java's <c>TimestampUtils</c>.
    /// </summary>
    internal static class TimestampUtils
    {
        public const string UnitMillis = "millis";
        public const string UnitMicros = "micros";
        public const string UnitNanos = "nanos";
        public const string UnitSeconds = "seconds";

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

        /// <summary>Construct from an epoch numeric value plus a unit string.</summary>
        public static Timestamp FromEpoch(long value, string unit)
        {
            if (unit == null)
            {
                throw new ArgumentException("timestamp.of unit must not be null");
            }

            switch (unit)
            {
                case UnitMillis: return FromEpochMillis(value);
                case UnitMicros: return FromEpochMicros(value);
                case UnitNanos: return FromEpochNanos(value);
                case UnitSeconds: return FromEpochSeconds(value);
                default:
                    throw new ArgumentException(
                        $"Unknown timestamp.of unit '{unit}'; expected one of millis, micros, nanos, seconds");
            }
        }

        /// <summary>Parse an RFC 3339 (offset-aware) timestamp string.</summary>
        public static Timestamp FromRfc3339(string s)
        {
            try
            {
                return Timestamp.FromDateTimeOffset(DateTimeOffset.Parse(
                    s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
            }
            catch (FormatException e)
            {
                throw new ArgumentException($"Cannot parse '{s}' as RFC 3339 timestamp", e);
            }
        }

        private static Timestamp FromDateTime(DateTime dt)
        {
            DateTime utc;
            switch (dt.Kind)
            {
                case DateTimeKind.Utc:
                    utc = dt;
                    break;
                case DateTimeKind.Local:
                    utc = dt.ToUniversalTime();
                    break;
                default:
                    // Avro `timestamp-millis`/`timestamp-micros` are UTC by spec but decode
                    // with Kind=Unspecified; treat them as UTC.
                    utc = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    break;
            }

            return Timestamp.FromDateTime(utc);
        }

        /// <summary>
        ///     Runtime dispatch backing <c>timestamp.of(dyn)</c>. Accepts the shapes Proto/Avro
        ///     decoders typically produce. A raw <see cref="long" /> lacks a unit and must use
        ///     <c>timestamp.of(value, unit)</c>.
        /// </summary>
        public static Timestamp ToTimestamp(object o)
        {
            switch (o)
            {
                case null:
                    throw new ArgumentException("Cannot convert null to Timestamp");
                case Timestamp ts:
                    return ts;
                case DateTime dt:
                    // Avro `timestamp-millis`/`timestamp-micros` decode to this.
                    return FromDateTime(dt);
                case DateTimeOffset dto:
                    return Timestamp.FromDateTimeOffset(dto);
                case Instant instant:
                    return Timestamp.FromDateTimeOffset(instant.ToDateTimeOffset());
                case ZonedDateTime zdt:
                    return Timestamp.FromDateTimeOffset(zdt.ToInstant().ToDateTimeOffset());
                case LocalDateTime _:
                    // Avro `local-timestamp-*` produces this; it carries no timezone. Refusing
                    // is more correct than silently assuming UTC.
                    throw new ArgumentException(
                        "Cannot convert LocalDateTime to Timestamp: local-timestamp values carry "
                        + "no timezone. Use the regular timestamp-* logical type (UTC by spec), or "
                        + "carry a TZ-offset field and use timestamp.of(value, unit).");
                case string s:
                    return FromRfc3339(s);
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                    throw new ArgumentException(
                        $"Cannot convert raw {o.GetType().Name} to Timestamp without a unit; use "
                        + "timestamp.of(value, \"millis\"|\"micros\"|\"nanos\"|\"seconds\")");
                default:
                    throw new ArgumentException($"Cannot convert {o.GetType().FullName} to Timestamp");
            }
        }
    }
}
