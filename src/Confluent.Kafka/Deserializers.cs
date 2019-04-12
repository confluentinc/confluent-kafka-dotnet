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
// Refer to LICENSE for more information.

using System;
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Deserializers for use with <see cref="Consumer{TKey,TValue}" />.
    /// </summary>
    public static class Deserializers
    {
        /// <summary>
        ///     String (UTF8 encoded) deserializer.
        /// </summary>
        public static IDeserializer<string> Utf8 = new Utf8Deserializer();
        
        private class Utf8Deserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
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
            }
        }

        /// <summary>
        ///     Null value deserializer.
        /// </summary>
        public static IDeserializer<Null> Null = new NullDeserializer();

        private class NullDeserializer : IDeserializer<Null>
        {
            public Null Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (!isNull)
                {
                    throw new ArgumentException("Deserializer<Null> may only be used to deserialize data that is null.");
                }

                return null;
            }
        }

        /// <summary>
        ///     Deserializer that deserializes any value to null.
        /// </summary>
        public static IDeserializer<Ignore> Ignore = new IgnoreDeserializer();

        private class IgnoreDeserializer : IDeserializer<Ignore>
        {
            public Ignore Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                => null;
        }

        /// <summary>
        ///     System.Int64 (big endian encoded, network byte ordered) deserializer.
        /// </summary>
        public static IDeserializer<long> Int64 = new Int64Deserializer();

        private class Int64Deserializer : IDeserializer<long>
        {
            public long Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull)
                {
                    throw new ArgumentNullException($"Null data encountered deserializing Int64 value.");
                }

                if (data.Length != 8)
                {
                    throw new ArgumentException($"Deserializer<Long> encountered data of length {data.Length}. Expecting data length to be 8.");
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
            }
        }

        /// <summary>
        ///     System.Int32 (big endian encoded, network byte ordered) deserializer.
        /// </summary>
        public static IDeserializer<int> Int32 = new Int32Deserializer();

        private class Int32Deserializer : IDeserializer<int>
        {
            public int Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull)
                {
                    throw new ArgumentNullException($"Null data encountered deserializing Int32 value");
                }

                if (data.Length != 4)
                {
                    throw new ArgumentException($"Deserializer<Int32> encountered data of length {data.Length}. Expecting data length to be 4.");
                }

                // network byte order -> big endian -> most significant byte in the smallest address.
                return
                    (((int)data[0]) << 24) |
                    (((int)data[1]) << 16) |
                    (((int)data[2]) << 8) |
                    (int)data[3];
            }
        }

        /// <summary>
        ///     System.Single (big endian encoded, network byte ordered) deserializer.
        /// </summary>
        public static IDeserializer<Single> Single = new SingleDeserializer();

        private class SingleDeserializer : IDeserializer<float>
        {
            public float Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull)
                {
                    throw new ArgumentNullException($"Null data encountered deserializing float value.");
                }

                if (data.Length != 4)
                {
                    throw new ArgumentException($"Deserializer<float> encountered data of length {data.Length}. Expecting data length to be 4.");
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
            }
        }

        /// <summary>
        ///     System.Double (big endian encoded, network byte ordered) deserializer.
        /// </summary>
        public static IDeserializer<Double> Double = new DoubleDeserializer();

        private class DoubleDeserializer : IDeserializer<double>
        {
            public double Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull)
                {
                    throw new ArgumentNullException($"Null data encountered deserializing double value.");
                }

                if (data.Length != 8)
                {
                    throw new ArgumentException($"Deserializer<double> encountered data of length {data.Length}. Expecting data length to be 8.");
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
            }
        }

        /// <summary>
        ///     System.Byte[] (nullable) deserializer.
        /// </summary>
        /// <remarks>
        ///     Byte ordering is original order.
        /// </remarks>
        public static IDeserializer<byte[]> ByteArray = new ByteArrayDeserializer();

        private class ByteArrayDeserializer : IDeserializer<byte[]>
        {
            public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull) { return null; }
                return data.ToArray();
            }
        }
    }
}
