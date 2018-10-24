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
    ///     Deserializers that can be used with <see cref="Confluent.Kafka.Consumer" />.
    /// </summary>
    public static class Deserializers
    {
        /// <summary>
        ///     Deserializes a UTF8 encoded string.
        /// </summary>
        public static Deserializer<string> UTF8 = (data, isNull) =>
        {
            if (isNull)
            {
                return null;
            }

            #if NETCOREAPP2_1
                return Encoding.UTF8.GetString(data);
            #else
                return Encoding.UTF8.GetString(data.ToArray());
            #endif
        };

        /// <summary>
        ///     Deserializes a null value to a null value.
        /// </summary>
        public static Deserializer<Null> Null = (data, isNull) =>
        {
            if (!isNull)
            {
                throw new System.ArgumentException("Deserializer<Null> may only be used to deserialize data that is null.");
            }

            return null;
        };

        /// <summary>
        ///     'Deserializes' any value to a null value.
        /// </summary>
        public static Deserializer<Ignore> Ignore = (data, isNull) => null;

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int64"/> value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized <see cref="System.Int64"/> value.
        /// </returns>
        public static Deserializer<long> Long = (ReadOnlySpan<byte> data, bool isNull) =>
        {
            if (isNull)
            {
                throw new ArgumentException($"Arg [{nameof(data)}] is null");
            }

            if (data.Length != 8)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by Deserializer<Long> is not 8");
            }

            // network byte order -> big endian -> most significant byte in the smallest address.
            long result = ((long)data[0]) << 56 |
                ((long)(data[1])) << 48 |
                ((long)(data[2])) << 40 |
                ((long)(data[3])) << 32 |
                ((long)(data[4])) << 24 |
                ((long)(data[5])) << 16 |
                ((long)(data[6])) << 8 |
                (data[7]);
            return result;
        };

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int32"/> value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized <see cref="System.Int32"/> value.
        /// </returns>
        public static Deserializer<int> Int32 = (ReadOnlySpan<byte> data, bool isNull) =>
        {
            if (isNull)
            {
                throw new ArgumentNullException($"Arg {nameof(data)} is null");
            }

            // network byte order -> big endian -> most significant byte in the smallest address.
            return
                (((int)data[0]) << 24) |
                (((int)data[1]) << 16) |
                (((int)data[2]) << 8) |
                (int)data[3];
        };

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Single value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized System.Single value.
        /// </returns>
        public static Deserializer<float> Float = (ReadOnlySpan<byte> data, bool isNull) =>
        {
            if (isNull)
            {
                throw new ArgumentNullException($"Arg {nameof(data)} is null");
            }

            if (data.Length != 4)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by Deserializer<Float> is not 4");
            }

            // network byte order -> big endian -> most significant byte in the smallest address.
            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    float result = default(float);
                    byte* p = (byte*)(&result);
                    *p++ = data[3];
                    *p++ = data[2];
                    *p++ = data[1];
                    *p++ = data[0];
                    return result;
                }
            }
            else
            {
#if NETCOREAPP2_1
                return BitConverter.ToSingle(data);
#else
                return BitConverter.ToSingle(data.ToArray(), 0);
#endif
            }
        };

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Double value from a byte array.
        /// </summary>
        /// <returns>
        ///     The deserialized System.Double value.
        /// </returns>
        public static Deserializer<double> Double = (ReadOnlySpan<byte> data, bool isNull) =>
        {
            if (isNull)
            {
                throw new ArgumentNullException($"Arg {nameof(data)} is null");
            }

            if (data.Length != 8)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by Deserializer<Double> is not 8");
            }

            // network byte order -> big endian -> most significant byte in the smallest address.
            if (BitConverter.IsLittleEndian)
            {
                unsafe
                {
                    double result = default(double);
                    byte* p = (byte*)(&result);
                    *p++ = data[7];
                    *p++ = data[6];
                    *p++ = data[5];
                    *p++ = data[4];
                    *p++ = data[3];
                    *p++ = data[2];
                    *p++ = data[1];
                    *p++ = data[0];
                    return result;
                }
            }
            else
            {
#if NETCOREAPP2_1
                return BitConverter.ToDouble(data);
#else
                return BitConverter.ToDouble(data.ToArray(), 0);
#endif
            }
        };

        /// <summary>
        ///     Deserializes a System.Byte[] value (or null).
        /// </summary>
        public static Deserializer<byte[]> ByteArray = (data, isNull) =>
        {
            if (isNull) { return null; }
            return data.ToArray();
        };
    }
}
