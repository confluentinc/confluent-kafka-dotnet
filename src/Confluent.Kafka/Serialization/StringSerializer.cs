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

        /// <summary>
        ///     Initializes a new StringSerializer class instance.
        ///     The encoding to use must be provided via a <see cref="Producer" /> 
        ///     configuration property. When used to serialize keys, the 
        ///     relevant property is 'dotnet.string.serializer.encoding.key'.
        ///     When used to serialize values, the relevant property is
        ///     'dotnet.string.serializer.encoding.value'. For available encodings, 
        ///     refer to:
        ///     https://msdn.microsoft.com/en-us/library/system.text.encoding(v=vs.110).aspx
        /// </summary>
        public StringSerializer()
        { }

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


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Serialization.ISerializer{T}.Configure(IEnumerable{KeyValuePair{string, object}}, bool)" />
        /// </summary>
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var propertyName = isKey ? ConfigPropertyNames.StringSerializerEncodingKey : ConfigPropertyNames.StringSerializerEncodingValue;
            var keyOrValue = isKey ? "Key" : "Value";

            if (config.Any(ci => ci.Key == propertyName))
            {
                if (encoding != null)
                {
                    throw new ArgumentException($"{keyOrValue} StringSerializer encoding was configured using both constructor and configuration parameter.");
                }

                string encodingName;
                try
                {
                    encodingName = config
                        .Single(ci => ci.Key == propertyName)
                        .Value
                        .ToString();
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"{keyOrValue} StringSerializer encoding configuration parameter was specified twice.", e);
                }
                
                encoding = encodingName.ToEncoding();

                return config.Where(ci => ci.Key != propertyName);
            }

            if (encoding == null)
            {
                throw new ArgumentException($"{keyOrValue} StringSerializer encoding was not configured.");
            }

            return config;
        }
        
        /// <summary>
        ///     Releases any unmanaged resources owned by the serializer (noop for this type).
        /// </summary>
        public void Dispose() {}
    }
}
