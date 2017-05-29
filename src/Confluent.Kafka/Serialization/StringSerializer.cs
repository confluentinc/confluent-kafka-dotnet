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
        private Encoding encoding;

        private const string KeyEncodingConfigParam = "dotnet.key.string.serializer.encoding";
        private const string ValueEncodingConfigParam = "dotnet.value.string.serializer.encoding";

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

        private static Encoding StringToEncoding(string encodingName) 
        {
            switch (encodingName)
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
                    throw new ArgumentException("unknown string encoding: " + encodingName);
            }
        }

        /// <summary>
        ///     Initializes a new StringSerializer class instance.
        ///     The encoding encoding to use must be provided via <see cref="Producer" /> configuration properties.
        /// </summary>
        public StringSerializer()
        {
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
        /// <returns>
        ///     <paramref name="data" /> encoded in a byte array (or null if <paramref name="data" /> is null).
        /// </returns>
        public byte[] Serialize(string topic, string data)
        {
            if (data == null)
            {
                return null;
            }

            return encoding.GetBytes(data);
        }


        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            if (isKey)
            {
                if (config.Count(ci => ci.Key == KeyEncodingConfigParam) > 0)
                {
                    var configItem = config.Single(ci => ci.Key == KeyEncodingConfigParam);
                    encoding = StringToEncoding(configItem.Value.ToString());
                    if (encoding != null)
                    {
                        throw new ArgumentException("Key StringSerializer encoding configured using both constructor and configuration parameter.");
                    }
                    return config.Where(ci => ci.Key != KeyEncodingConfigParam);
                }
                if (encoding == null)
                {
                    throw new ArgumentException("Key StringSerializer encoding was not configured");
                }
                return config;
            }
            else
            {
                if (config.Count(ci => ci.Key == ValueEncodingConfigParam) > 0)
                {
                    var configItem = config.Single(ci => ci.Key == ValueEncodingConfigParam);
                    encoding = StringToEncoding(configItem.Value.ToString());
                    if (encoding != null)
                    {
                        throw new ArgumentException("Value StringSerializer encoding configured using both constructor and configuration parameter.");
                    }
                    return config.Where(ci => ci.Key != ValueEncodingConfigParam);
                }
                if (encoding == null)
                {
                    throw new ArgumentException("Value StringSerializer encoding was not configured");
                }
                return config;
            }
        }
    }
}
