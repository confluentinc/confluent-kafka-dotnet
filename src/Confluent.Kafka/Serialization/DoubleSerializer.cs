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
    ///     System.Double serializer. Byte order of serialized data is big endian (network byte order).
    /// </summary>
    public class DoubleSerializer : ISerializer<double>
    {
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
        public byte[] Serialize(string topic, double data)
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

        /// <include file='../include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;

        /// <summary>
        ///     Releases any unmanaged resources owned by the serializer (noop for this type).
        /// </summary>
        public void Dispose() {}
    }
}
