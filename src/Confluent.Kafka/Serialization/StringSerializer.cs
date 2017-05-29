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
using System.Linq;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A serializer for string values.
    /// </summary>
    public class StringSerializer : ISerializer<string>
    {
        private Encoding keyEncoding;
        private Encoding valueEncoding;

        /// <summary>
        ///     Initializes a new StringSerializer class instance.
        /// </summary>
        /// <param name="encoding">
        ///     The encoding to use when serializing.
        /// </param>
        public StringSerializer(Encoding encoding)
        {
            this.keyEncoding = encoding;
            this.valueEncoding = encoding;
        }

        private static Encoding StringToEncoding(string s) 
        {
            switch (s)
            {
                case "UTF8":
                    return Encoding.UTF8;
                case "UTF7":
                    return Encoding.UTF7;
                case "UTF32":
                    return Encoding.UTF32;
                case "ASCII":
                    return Encoding.ASCII;
                case "Unicode":
                    return Encoding.Unicode;
                case "BigEndignUnicode":
                    return Encoding.BigEndianUnicode;
                default:
                    throw new ArgumentException("unknown string encoding: " + s);
            }
        }

        /// <summary>
        ///     Initializes a new StringSerializer class instance.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public StringSerializer(IEnumerable<KeyValuePair<string, object>> config)
        {
            if (config.Count(ci => ci.Key == "string.serializer.encoding.key") > 0)
            {
                var configItem = config.Single(ci => ci.Key == "string.serializer.encoding.key");
                keyEncoding = StringToEncoding(configItem.Value.ToString());
                configuration.Add(configItem.Key, configItem.Value);
            }

            if (config.Count(ci => ci.Key == "string.serializer.encoding.value") > 0)
            {
                var configItem = config.Single(ci => ci.Key == "string.serializer.encoding.value");
                valueEncoding = StringToEncoding(configItem.Value.ToString());
                configuration.Add(configItem.Key, configItem.Value);
            }
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
            if (data == null)
            {
                return null;
            }

            return isKey 
                ? keyEncoding.GetBytes(data) 
                : valueEncoding.GetBytes(data);
        }

        private Dictionary<string, object> configuration = new Dictionary<string, object>();

        /// <summary>
        ///     Configuration properties handled by the serializer.
        /// </summary>
        public IEnumerable<KeyValuePair<string, object>> Configuration 
            => configuration;
    }
}
