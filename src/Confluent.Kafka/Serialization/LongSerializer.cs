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
    ///     A serializer for <see cref="System.Int64"/> values. The byte order of serialized data is big endian (network byte order).
    /// </summary>
    public class LongSerializer : ISerializer<long>
    {
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
        public byte[] Serialize(string topic, long data)
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
        ///     Refer to <see cref="Confluent.Kafka.Serialization.ISerializer{T}.Configure(IEnumerable{KeyValuePair{string, string}}, bool)" />
        /// </summary>
        public IEnumerable<KeyValuePair<string, string>> Configure(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
            => config;
    }
}
