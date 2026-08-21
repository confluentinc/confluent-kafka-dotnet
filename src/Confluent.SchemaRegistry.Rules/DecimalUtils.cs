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
using System.Numerics;
using Avro;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Conversion helpers backing <c>decimal(...)</c> and the <c>decimals.*</c> operator
    ///     functions. The CEL surface treats Decimal as the canonical type
    ///     <see cref="CelTypeLabels.DecimalName" />; this client backs it with
    ///     <see cref="BigDecimal" />. This is the CEL counterpart of Java's
    ///     <c>DecimalUtils</c>; the serde-side <c>System.Decimal</c> converter lives
    ///     separately in <c>Confluent.SchemaRegistry.Serdes.DecimalExtensions</c>.
    /// </summary>
    internal static class DecimalUtils
    {
        /// <summary>
        ///     Decode unscaled big-endian two's-complement bytes into a
        ///     <see cref="BigInteger" />. Empty bytes map to zero (matching an empty
        ///     <c>confluent.type.Decimal.value</c>).
        /// </summary>
        private static BigInteger FromUnscaledBytes(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
            {
                return BigInteger.Zero;
            }

            // .NET's BigInteger(byte[]) reads little-endian; the wire form is big-endian
            // (Java BigInteger.toByteArray()), so reverse before decoding.
            var le = (byte[])bytes.Clone();
            Array.Reverse(le);
            return new BigInteger(le);
        }

        /// <summary>Build a <see cref="BigDecimal" /> from an unscaled integer and a scale.</summary>
        public static BigDecimal ToBigDecimal(BigInteger unscaled, int scale)
        {
            return new BigDecimal(unscaled, scale);
        }

        /// <summary>Build a <see cref="BigDecimal" /> from raw two's-complement bytes plus scale.</summary>
        public static BigDecimal ToBigDecimal(byte[] bytes, int scale)
        {
            return ToBigDecimal(FromUnscaledBytes(bytes), scale);
        }

        /// <summary>
        ///     Decode any <c>confluent.type.Decimal</c> message — a concrete generated class
        ///     or the dynamic <see cref="IMessage" /> a runtime-parsed schema produces — by
        ///     reading its <c>value</c> and <c>scale</c> fields off the descriptor.
        /// </summary>
        public static BigDecimal ToBigDecimal(IMessage message)
        {
            MessageDescriptor descriptor = message.Descriptor;
            FieldDescriptor valueField = descriptor.FindFieldByName("value");
            FieldDescriptor scaleField = descriptor.FindFieldByName("scale");
            if (valueField == null || scaleField == null)
            {
                throw new ArgumentException(
                    "confluent.type.Decimal message missing required field: "
                    + (valueField == null ? "'value'" : "'scale'"));
            }

            var unscaled = (ByteString)valueField.Accessor.GetValue(message);
            int scale = Convert.ToInt32(scaleField.Accessor.GetValue(message), CultureInfo.InvariantCulture);
            return ToBigDecimal(FromUnscaledBytes(unscaled == null ? null : unscaled.ToByteArray()), scale);
        }

        /// <summary>
        ///     Runtime dispatch backing <c>decimal(dyn)</c>. Accepts whatever shape a
        ///     Proto/Avro decoder commonly produces. Throws with a hint when the input lacks
        ///     the scale metadata a Decimal needs (raw bytes must use <c>decimal(bytes, scale)</c>).
        /// </summary>
        public static BigDecimal ToBigDecimal(object o)
        {
            switch (o)
            {
                case null:
                    throw new ArgumentException("Cannot convert null to Decimal");
                case BigDecimal d:
                    return d;
                case AvroDecimal ad:
                    // Avro logical `decimal` decodes to this (unscaled BigInteger + scale).
                    return ToBigDecimal(ad.UnscaledValue, ad.Scale);
                case IMessage msg when msg.Descriptor.FullName == CelTypeLabels.DecimalName:
                    return ToBigDecimal(msg);
                case bool _:
                    // bool is not numeric; guard before the integer arms below.
                    throw new ArgumentException("Cannot convert Boolean to Decimal");
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case long _:
                    return BigDecimal.FromLong(Convert.ToInt64(o, CultureInfo.InvariantCulture));
                case uint u:
                    return BigDecimal.FromLong(u);
                case ulong u:
                    return BigDecimal.FromBigInteger(new BigInteger(u));
                case BigInteger bi:
                    return BigDecimal.FromBigInteger(bi);
                case double dbl:
                    return BigDecimal.FromDouble(dbl);
                case float f:
                    // Float precision (not widened to double) so 0.1f → "0.1", with the
                    // whole-number ".0" preserved (scale 1) to match Java/Python.
                    return BigDecimal.FromFloat(f);
                case string s:
                    return BigDecimal.Parse(s);
                case byte[] _:
                case ByteString _:
                    throw new ArgumentException(
                        "Cannot convert raw bytes to Decimal without a scale; use "
                        + "decimal(bytes, scale)");
                default:
                    throw new ArgumentException($"Cannot convert {o.GetType().FullName} to Decimal");
            }
        }
    }
}
