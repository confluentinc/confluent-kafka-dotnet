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
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
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
            Span<byte> bytes = stackalloc byte[16];
            WriteBytesFromDecimal(value, bytes);

            // Copy the 12 bytes into an array of size 13 so that the last byte is 0,
            // which will ensure that the unscaled value is positive.
            Span<byte> unscaledValueBytes = stackalloc byte[13];
            bytes.Slice(0, 12).CopyTo(unscaledValueBytes);

#if NET6_0_OR_GREATER
            var unscaledValue = new BigInteger(unscaledValueBytes);
#else
            var unscaledValue = new BigInteger(unscaledValueBytes.ToArray());
#endif
            
            if (bytes[15] == 128)
            {
                unscaledValue *= BigInteger.MinusOne;
            }
            var scale = bytes[14];
            
#if NET6_0_OR_GREATER
            Span<byte> buffer = stackalloc byte[16];
            unscaledValue.TryWriteBytes(buffer, out var bytesWritten, isBigEndian: true);
            buffer = buffer.Slice(0, bytesWritten);
#else
            var buffer = unscaledValue.ToByteArray();
            Array.Reverse(buffer);
#endif
            
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
#if NET6_0_OR_GREATER
            var unscaledValue = new BigInteger(value.Value.Span, isBigEndian: true);
#else
            var buffer = value.Value.ToByteArray();
            Array.Reverse(buffer);
            var unscaledValue = new BigInteger(buffer);
#endif

            var scaleDivisor = BigInteger.Pow(new BigInteger(10), value.Scale);
            var quotient = BigInteger.DivRem(unscaledValue, scaleDivisor, out var remainder);

            if (quotient > new BigInteger(decimal.MaxValue))
            {
                throw new OverflowException($"The value {unscaledValue} cannot fit into decimal.");
            }

            var leftOfDecimal = (decimal)quotient;
            var rightOfDecimal = (decimal)remainder / (decimal)scaleDivisor;

            return leftOfDecimal + rightOfDecimal;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteBytesFromDecimal(decimal value, Span<byte> destination)
        {
#if NET6_0_OR_GREATER
            Span<int> bits = stackalloc int[4];
            _ = decimal.GetBits(value, bits);
#else
            var bits = decimal.GetBits(value);
#endif 
            
            BinaryPrimitives.WriteInt32LittleEndian(destination, bits[0]);
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4), bits[1]);
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8), bits[2]);
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12), bits[3]);
        }
    }
}
