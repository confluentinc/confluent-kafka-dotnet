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

        private const string KeyEncodingConfigParam = "dotnet.string.serializer.encoding.key";
        private const string ValueEncodingConfigParam = "dotnet.string.serializer.encoding.value";

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

        /// <include file='../include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var propertyName = isKey ? KeyEncodingConfigParam : ValueEncodingConfigParam;
            var keyOrValue = isKey ? "Key" : "Value";

            if (config.Count(ci => ci.Key == propertyName) > 0)
            {
                try
                {
                    encoding = config
                        .Single(ci => ci.Key == propertyName)
                        .Value
                        .ToString()
                        .ToEncoding();
                }
                catch
                {
                    throw new ArgumentException($"{keyOrValue} StringSerializer encoding configuration parameter was specified twice.");
                }

                if (encoding != null)
                {
                    throw new ArgumentException($"{keyOrValue} StringSerializer encoding was configured using both constructor and configuration parameter.");
                }

                return config.Where(ci => ci.Key != KeyEncodingConfigParam);
            }

            if (encoding == null)
            {
                throw new ArgumentException($"{keyOrValue} StringSerializer encoding was not configured.");
            }

            return config;
        }

    }
}
