// Copyright 2016-2017 Confluent Inc.
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
using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A deserializer for big endian encoded (network byte ordered) System.Double values.
    /// </summary>
    public class DoubleDeserializer : IDeserializer<double>
    {
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
        public double Deserialize(string topic, ReadOnlySpan<byte> data, bool isNull)
        {
            if (isNull)
            {
                throw new ArgumentNullException($"Arg {nameof(data)} is null");
            }

            if (data.Length != 8)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by {nameof(DoubleDeserializer)} is not 8");
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
        ///     Refer to <see cref="Confluent.Kafka.Serialization.IDeserializer{T}.Configure(IEnumerable{KeyValuePair{string, object}}, bool)" />
        /// </summary>
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;

        /// <summary>
        ///     Releases any unmanaged resources owned by the deserializer (noop for this type).
        /// </summary>
        public void Dispose() {}
    }
}
