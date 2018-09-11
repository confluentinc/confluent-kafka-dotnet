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

using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     System.Byte[] serializer. This serializer simply passes 
    ///     through the provided System.Byte[] value.
    /// </summary>
    public class ByteArraySerializer : ISerializer<byte[]>
    {
        /// <summary>
        ///     Serializes the specified System.Byte[] value (or null) to 
        ///     a byte array. Byte order is original order. 
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <param name="data">
        ///     The System.Byte[] value to serialize (or null).
        /// </param>
        /// <returns>
        ///     The System.Byte[] value <paramref name="data" /> encoded as a byte array. 
        /// </returns>
        public byte[] Serialize(string topic, byte[] data)
        {
            return data;
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Serialization.ISerializer{T}.Configure(IEnumerable{KeyValuePair{string, string}}, bool)" />
        /// </summary>
        public IEnumerable<KeyValuePair<string, string>> Configure(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
            => config;
    }
}
