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
    ///     A deserializer for System.Byte[] values. This deserializer creates a System.Byte[] 
    ///     from the 
    /// </summary>
    public class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        /// <summary>
        ///     Deserializes a System.Byte[] value (or null) from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the serialized System.Byte[] value (or null).
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized System.Byte[] value.
        /// </returns>
        public byte[] Deserialize(string topic, ReadOnlySpan<byte> data, bool isNull)
        {
            if (isNull) { return null; }
            return data.ToArray();
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
