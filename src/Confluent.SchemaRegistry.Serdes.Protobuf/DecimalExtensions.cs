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
            var bytes = GetBytesFromDecimal(value);

            // Copy the 12 bytes into an array of size 13 so that the last byte is 0,
            // which will ensure that the unscaled value is positive.
            var unscaledValueBytes = new byte[13];
            Array.Copy(bytes, unscaledValueBytes, 12);

            var unscaledValue = new BigInteger(unscaledValueBytes);
            var scale = bytes[14];

            if (bytes[15] == 128)
            {
                unscaledValue *= BigInteger.MinusOne;
            }

            var buffer = unscaledValue.ToByteArray();
            Array.Reverse(buffer);

            return new Decimal {
                Value = ByteString.CopyFrom(buffer),
                Scale = scale,
            };
        }

        /// <summary>
        ///   Converts a Protobuf decimal to decimal
        /// </summary>
        /// <param name="value">Protobuf decimal value</param>
        /// <returns>Decimal value</returns>
        public static decimal ToSystemDecimal(this Decimal value)
        {
            var buffer = value.Value.ToByteArray();
            Array.Reverse(buffer);

            var unscaledValue = new BigInteger(buffer);

            var scaleDivisor = BigInteger.Pow(new BigInteger(10), value.Scale);
            var remainder = BigInteger.Remainder(unscaledValue, scaleDivisor);
            var scaledValue = BigInteger.Divide(unscaledValue, scaleDivisor);

            if (scaledValue > new BigInteger(decimal.MaxValue))
            {
                throw new OverflowException($"The value {unscaledValue} cannot fit into decimal.");
            }

            var leftOfDecimal = (decimal)scaledValue;
            var rightOfDecimal = ((decimal)remainder) / ((decimal)scaleDivisor);

            return leftOfDecimal + rightOfDecimal;
        }

        /// <summary>
        /// Gets the bytes from decimal.
        /// </summary>
        /// <param name="d">The <see cref="decimal" />.</param>
        /// <returns>
        /// A byte array.
        /// </returns>
        private static byte[] GetBytesFromDecimal(decimal d)
        {
            byte[] bytes = new byte[16];

            int[] bits = decimal.GetBits(d);
            int lo = bits[0];
            int mid = bits[1];
            int hi = bits[2];
            int flags = bits[3];

            bytes[0] = (byte)lo;
            bytes[1] = (byte)(lo >> 8);
            bytes[2] = (byte)(lo >> 0x10);
            bytes[3] = (byte)(lo >> 0x18);
            bytes[4] = (byte)mid;
            bytes[5] = (byte)(mid >> 8);
            bytes[6] = (byte)(mid >> 0x10);
            bytes[7] = (byte)(mid >> 0x18);
            bytes[8] = (byte)hi;
            bytes[9] = (byte)(hi >> 8);
            bytes[10] = (byte)(hi >> 0x10);
            bytes[11] = (byte)(hi >> 0x18);
            bytes[12] = (byte)flags;
            bytes[13] = (byte)(flags >> 8);
            bytes[14] = (byte)(flags >> 0x10);
            bytes[15] = (byte)(flags >> 0x18);

            return bytes;
        }
    }
}
