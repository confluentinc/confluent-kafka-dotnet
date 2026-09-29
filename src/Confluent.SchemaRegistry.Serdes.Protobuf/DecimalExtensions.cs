// Copyright 2025 Confluent Inc.
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
using Google.Protobuf;
using Decimal = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///   Protobuf decimal extensions
    /// </summary>
    public static class DecimalExtensions
    {
        /// <summary>
        ///   Converts a decimal to a Protobuf decimal
        /// </summary>
        /// <param name="value">Decimal value</param>
        /// <returns>Protobuf decimal value</returns>
        public static Decimal ToProtobufDecimal(this decimal value)
        {
            return BigDecimal.FromDecimal(value).ToProtobufDecimal();
        }

        /// <summary>
        ///   Converts a Protobuf decimal to decimal
        /// </summary>
        /// <param name="value">Protobuf decimal value</param>
        /// <returns>Decimal value</returns>
        public static decimal ToSystemDecimal(this Decimal value)
        {
            return value.ToBigDecimal().ToDecimal();
        }

        /// <summary>
        ///   Converts a Protobuf decimal to a <see cref="BigDecimal" /> (lossless)
        /// </summary>
        /// <param name="value">Protobuf decimal value</param>
        /// <returns>BigDecimal value</returns>
        public static BigDecimal ToBigDecimal(this Decimal value)
        {
#if NET6_0_OR_GREATER
            var unscaled = new BigInteger(value.Value.Span, isBigEndian: true);
#else
            var buffer = value.Value.ToByteArray();
            Array.Reverse(buffer);
            // An unset protobuf decimal carries ByteString.Empty, and BigInteger(byte[]) rejects
            // an empty array on these targets. DecimalUtils.FromUnscaledBytes maps it to zero.
            var unscaled = buffer.Length == 0 ? BigInteger.Zero : new BigInteger(buffer);
#endif
            return new BigDecimal(unscaled, value.Scale);
        }

        /// <summary>
        ///   Converts a <see cref="BigDecimal" /> to a Protobuf decimal (lossless)
        /// </summary>
        /// <param name="value">BigDecimal value</param>
        /// <returns>Protobuf decimal value</returns>
        public static Decimal ToProtobufDecimal(this BigDecimal value)
        {
            var buffer = value.Unscaled.ToByteArray(); // little-endian two's-complement, minimal
            Array.Reverse(buffer);                      // big-endian wire form
            // Precision is the unscaled value's digit count, as Java's
            // DecimalUtils.fromBigDecimal sets it (BigDecimal.precision()). Zero has precision 1.
            // Through BigDecimal.UnscaledPrecision, the one guarded definition, so this path
            // and the CEL write-back agree on which coefficients can be written: the digit
            // count is a quadratic BigInteger.ToString(), which is what SaneCoefficient bounds.
            // Counted there from the byte length rather than GetBitLength(), which does not
            // exist on this project's netstandard2.0 and net462 targets.
            var precision = BigDecimal.UnscaledPrecision(value.Unscaled);
            return new Decimal
            {
                Value = ByteString.CopyFrom(buffer),
                Precision = precision,
                Scale = value.Scale
            };
        }
    }
}
