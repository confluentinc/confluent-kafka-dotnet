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
    ///     A deserializer for big endian encoded (network byte ordered) System.Single values.
    /// </summary>
    public class FloatDeserializer : IDeserializer<float>
    {
        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Single value from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the serialized System.Single value (big endian encoding).
        /// </param>
        /// <returns>
        ///     The deserialized System.Single value.
        /// </returns>
        public float Deserialize(string topic, byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException($"Arg {nameof(data)} is null");
            }

            if (data.Length != 4)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by {nameof(FloatDeserializer)} is not 4");
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
                return BitConverter.ToSingle(data, 0);
            }
        }

        /// <include file='../include_docs.xml' path='API/Member[@name="IDeserializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;
    }
}
