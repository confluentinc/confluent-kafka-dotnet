// Copyright 2018 Confluent Inc.
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
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Serializers that can be used with <see cref="Confluent.Kafka.Producer" />.
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        ///     Encodes a string value into a byte array.
        /// </summary>
        public static Serializer<string> UTF8 = (data) =>
        {
            if (data == null)
            {
                return null;
            }

            return Encoding.UTF8.GetBytes(data);
        };

        /// <summary>
        ///     Encodes a Null value to null.
        /// </summary>
        public static Serializer<Null> Null = (data) => null;

        /// <summary>
        ///     Serializes the specified <see cref="System.Int64"/> value to a byte array of length 8. Byte order is big endian (network byte order).
        /// </summary>
        public static Serializer<long> Long = (data) =>
        {
            var result = new byte[8];
            result[0] = (byte)(data >> 56);
            result[1] = (byte)(data >> 48);
            result[2] = (byte)(data >> 40);
            result[3] = (byte)(data >> 32);
            result[4] = (byte)(data >> 24);
            result[5] = (byte)(data >> 16);
            result[6] = (byte)(data >> 8);
            result[7] = (byte)data;
            return result;
        };

        /// <summary>
        ///     Serializes the specified <see cref="System.Int32"/> value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        public static Serializer<int> Int32 = (data) =>
        {
            var result = new byte[4]; // int is always 32 bits on .NET.
            // network byte order -> big endian -> most significant byte in the smallest address.
            // Note: At the IL level, the conv.u1 operator is used to cast int to byte which truncates
            // the high order bits if overflow occurs.
            // https://msdn.microsoft.com/en-us/library/system.reflection.emit.opcodes.conv_u1.aspx
            result[0] = (byte)(data >> 24);
            result[1] = (byte)(data >> 16); // & 0xff;
            result[2] = (byte)(data >> 8); // & 0xff;
            result[3] = (byte)data; // & 0xff;
            return result;
        };

        /// <summary>
        ///     Serializes the specified System.Single value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        /// <returns>
        ///     The System.Single value encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public static Serializer<float> Float = (data) =>
        {
            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    byte[] result = new byte[4];
                    byte* p = (byte*)(&data);
                    result[3] = *p++;
                    result[2] = *p++;
                    result[1] = *p++;
                    result[0] = *p++;
                    return result;
                }
            }
            else
            {
                return BitConverter.GetBytes(data);
            }
        };

        /// <summary>
        ///     Serializes the specified System.Double value to a byte array of length 8. Byte order is big endian (network byte order).
        /// </summary>
        public static Serializer<double> Double = (data) =>
        {
            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    byte[] result = new byte[8];
                    byte* p = (byte*)(&data);
                    result[7] = *p++;
                    result[6] = *p++;
                    result[5] = *p++;
                    result[4] = *p++;
                    result[3] = *p++;
                    result[2] = *p++;
                    result[1] = *p++;
                    result[0] = *p++;
                    return result;
                }
            }
            else
            {
                return BitConverter.GetBytes(data);
            }
        };

        /// <summary>
        ///     Serializes the specified System.Byte[] value (or null) to 
        ///     a byte array. Byte order is original order. 
        /// </summary>
        public static Serializer<byte[]> ByteArray = (data) => data;
    }
}
