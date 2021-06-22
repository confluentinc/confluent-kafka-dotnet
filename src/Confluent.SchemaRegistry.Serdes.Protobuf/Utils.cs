// Copyright 2020 Confluent Inc.
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
using System.IO;


namespace Confluent.SchemaRegistry.Serdes
{
    internal static class Utils
    {
        public static void WriteVarint(this Stream stream, uint value) {
            WriteUnsignedVarint(stream, (value << 1) ^ (value >> 31));
        }
        
        /// <remarks>
        ///     Inspired by: https://github.com/apache/kafka/blob/2.5/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java#L284
        /// </remarks>
        public static void WriteUnsignedVarint(this Stream stream, uint value)
        {
            while ((value & 0xffffff80) != 0L) {
                byte b = (byte) ((value & 0x7f) | 0x80);
                stream.WriteByte(b);
                value >>= 7;
            }
            stream.WriteByte((byte) value);
        }
        public static int ReadVarint(this Stream stream)  {
            var value = ReadUnsignedVarint(stream);
            return (int)((value >> 1) ^ -(value & 1));
        }
        
        /// <remarks>
        ///     Inspired by: https://github.com/apache/kafka/blob/2.5/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java#L142
        /// </remarks>
        public static uint ReadUnsignedVarint(this Stream stream)
        {
            int value = 0;
            int i = 0;
            int b;
            while (true) {
                b = stream.ReadByte();
                if (b == -1) throw new InvalidOperationException("Unexpected end of stream reading varint.");
                if ((b & 0x80) == 0) { break; }
                value |= (b & 0x7f) << i;
                i += 7;
                if (i > 28)
                {
                    throw new OverflowException($"Encoded varint is larger than uint.MaxValue");
                }
            }
            value |= b << i;
            return (uint)value;
        }
    }
}
