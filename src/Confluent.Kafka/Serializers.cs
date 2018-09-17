using System;
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Serializers that can be used with <see cref="Confluent.Kafka.Producer{TKey, TValue}" />.
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        ///     Encodes a string value in a byte array.
        /// </summary>
        public static Serializer<string> UTF8 = (topic, data) =>
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
        public static Serializer<Null> Null = (topic, data) => null;

        /// <summary>
        ///     Serializes the specified <see cref="System.Int64"/> value to a byte array of length 8. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="data">
        ///     The <see cref="System.Int64"/> value to serialize.
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <returns>
        ///     The <see cref="System.Int64"/> value <paramref name="data" /> encoded as a byte array of length 8 (network byte order).
        /// </returns>
        public static byte[] Long(string topic, long data)
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
        }

        /// <summary>
        ///     Serializes the specified <see cref="System.Int32"/> value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="data">
        ///     The <see cref="System.Int32"/> value to serialize.
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <returns>
        ///     The <see cref="System.Int32"/> value <paramref name="data" /> encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public static byte[] Int32(string topic, int data)
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
        }

        /// <summary>
        ///     Serializes the specified System.Single value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <param name="data">
        ///     The System.Single value to serialize.
        /// </param>
        /// <returns>
        ///     The System.Single value <paramref name="data" /> encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public static byte[] Float(string topic, float data)
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
        }

        /// <summary>
        ///     Serializes the specified System.Double value to a byte array of length 8. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <param name="data">
        ///     The System.Double value to serialize.
        /// </param>
        /// <returns>
        ///     The System.Double value <paramref name="data" /> encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public static byte[] Double(string topic, double data)
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
        }

        /// <summary>
        ///     Serializes the specified System.Byte[] value (or null) to 
        ///     a byte array. Byte order is original order. 
        /// </summary>
        public static Serializer<byte[]> ByteArray = (topic, data) => data;

        /// <summary>
        ///     Generators for the standard serializers.
        /// </summary>
        public static class Generators
        {
            /// <summary>
            ///     Generates a UTF8 serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<string> UTF8 = (forKey) => Serializers.UTF8;

            /// <summary>
            ///     Generates a Null serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<Null> Null = (forKey) => Serializers.Null;

            /// <summary>
            ///     Generates a Int32 serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<Int32> Int32 = (forKey) => Serializers.Int32;

            /// <summary>
            ///     Generates a Long serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<long> Long = (forKey) => Serializers.Long;

            /// <summary>
            ///     Generates a Float serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<float> Float = (forKey) => Serializers.Float;

            /// <summary>
            ///     Generates a Double serializer (invariant on the value of forKey).
            /// </summary>
            public static SerializerGenerator<double> Double = (forKey) => Serializers.Double;
        }
    }
}
