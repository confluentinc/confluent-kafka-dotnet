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
using System.Text;
using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A serializer for string values.
    /// </summary>
    public class StringSerializer : ISerializer<string>
    {
        private Encoding encoding;

        /// <summary>
        ///     Initializes a new StringSerializer class instance.
        /// </summary>
        /// <param name="encoding">
        ///     The encoding to use when serializing.
        /// </param>
        public StringSerializer(Encoding encoding)
        {
            this.encoding = encoding;
        }

        private byte[] Serialize(string val)
        {
            if (val == null)
            {
                return null;
            }
            return encoding.GetBytes(val);
        }

        /// <summary>
        ///     Encodes a string value in a byte array.
        /// </summary>
        /// <param name="data">
        ///     The string value to serialize.
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <param name="isKey">
        ///     true: deserialization is for a key, 
        ///     false: deserializing is for a value.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> encoded in a byte array (or null if <paramref name="data" /> is null).
        /// </returns>
        public byte[] Serialize(string topic, string data, bool isKey)
        {
            return Serialize(data);
        }

        /// <summary>
        ///     Configuration properties used by the serializer.
        /// </summary>
        public IEnumerable<KeyValuePair<string, object>> Configuration 
            => new List<KeyValuePair<string, object>>();
    }
}
