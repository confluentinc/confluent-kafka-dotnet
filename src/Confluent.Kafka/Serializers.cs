// Copyright 2018-2019 Confluent Inc.
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
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Serializers for use with <see cref="Producer{TKey,TValue}" />.
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        ///     String (UTF8) serializer.
        /// </summary>
        public static Serializer<string> Utf8 = data =>
        {
            if (data == null)
            {
                return null;
            }

            return Encoding.UTF8.GetBytes(data);
        };


        /// <summary>
        ///     Null serializer.
        /// </summary>
        public static Serializer<Null> Null = data => null;


        /// <summary>
        ///     System.Int64 (big endian, network byte order) serializer.
        /// </summary>
        public static Serializer<long> Int64 = data =>
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
        ///     System.Int32 (big endian, network byte order) serializer.
        /// </summary>
        public static Serializer<int> Int32 = data =>
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
        ///     System.Single (big endian, network byte order) serializer
        /// </summary>
        public static Serializer<float> Single = data =>
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
        ///     System.Double (big endian, network byte order) serializer
        /// </summary>
        public static Serializer<double> Double = data =>
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
        ///     System.Byte[] (nullable) serializer.
        /// </summary>
        /// <remarks>
        ///     Byte order is original order.
        /// </remarks>
        public static Serializer<byte[]> ByteArray = data => data;        
    }
}
