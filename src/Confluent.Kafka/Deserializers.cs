using System;
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Deserializers that can be used with <see cref="Confluent.Kafka.Consumer{TKey, TValue}" />.
    /// </summary>
    public static class Deserializers
    {
        /// <summary>
        ///     Deserializes a UTF8 encoded string.
        /// </summary>
        public static Deserializer<string> UTF8 = (topic, data, isNull) =>
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
        public static Deserializer<Null> Null = (topic, data, isNull) =>
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
        public static Deserializer<Ignore> Ignore = (topic, data, isNull) => null;

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int64"/> value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     A byte array containing the serialized <see cref="System.Int64"/> value (big endian encoding)
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized <see cref="System.Int64"/> value.
        /// </returns>
        public static long Long(string topic, ReadOnlySpan<byte> data, bool isNull)
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
        }

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int32"/> value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     A byte array containing the serialized <see cref="System.Int32"/> value (big endian encoding).
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized <see cref="System.Int32"/> value.
        /// </returns>
        public static int Int32(string topic, ReadOnlySpan<byte> data, bool isNull)
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
        }

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Single value from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the serialized System.Single value (big endian encoding).
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized System.Single value.
        /// </returns>
        public static float Float(string topic, ReadOnlySpan<byte> data, bool isNull)
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
        }

        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Double value from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the serialized System.Double value (big endian encoding).
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized System.Double value.
        /// </returns>
        public static double Double(string topic, ReadOnlySpan<byte> data, bool isNull)
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
        }

        /// <summary>
        ///     Deserializes a System.Byte[] value (or null).
        /// </summary>
        public static Deserializer<byte[]> ByteArray = (topic, data, isNull) =>
        {
            if (isNull) { return null; }
            return data.ToArray();
        };

        /// <summary>
        ///     Generators for the standard deserializers (simply return the 
        ///     appropriate deserializer regardless of the value of the forKey
        ///     parameter)
        /// </summary>
        public static class Generators
        {
            /// <summary>
            ///     Generates a UTF8 deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<string> UTF8 = (forKey) => Deserializers.UTF8;

            /// <summary>
            ///     Generates a ByteArray deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<byte[]> ByteArray = (forKey) => Deserializers.ByteArray;

            /// <summary>
            ///     Generates a Double deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<double> Double = (forKey) => Deserializers.Double;

            /// <summary>
            ///     Generates a Float deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<float> Float = (forKey) => Deserializers.Float;

            /// <summary>
            ///     Generates a Int32 deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<int> Int32 = (forKey) => Deserializers.Int32;

            /// <summary>
            ///     Generates a Long deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<long> Long = (forKey) => Deserializers.Long;

            /// <summary>
            ///     Generates a Null deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<Null> Null = (forKey) => Deserializers.Null;

            /// <summary>
            ///     Generates a Ignore deserializer (invariant on the value of forKey)
            /// </summary>
            public static DeserializerGenerator<Ignore> Ignore = (forKey) => Deserializers.Ignore;
        }
    }
}
