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
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A deserializer for string values.
    /// </summary>
    public class StringDeserializer : IDeserializer<string>
    {
        Encoding encoding;

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding when deserializing keys.
        /// </summary>
        public const string KeyEncodingConfigParam = "dotnet.string.deserializer.encoding.key";

        /// <summary>
        ///     Name of the configuration parameter used to specify the encoding when deserializing values.
        /// </summary>
        public const string ValueEncodingConfigParam = "dotnet.string.deserializer.encoding.value";


        /// <summary>
        ///     Initializes a new StringDeserializer class instance.
        /// </summary>
        /// <param name="encoding">
        ///     The encoding to use when deserializing.
        /// </param>
        public StringDeserializer(Encoding encoding)
        {
            this.encoding = encoding;
        }

        /// <summary>
        ///     Initializes a new StringDeserializer class instance.
        ///     The encoding to use must be provided via a <see cref="Consumer" /> 
        ///     configuration property. When used to deserialize keys, the 
        ///     relevant property is 'dotnet.string.deserializer.encoding.key'.
        ///     When used to deserialize values, the relevant property is
        ///     'dotnet.string.deserializer.encoding.value'. For available encodings, 
        ///     refer to:
        ///     https://msdn.microsoft.com/en-us/library/system.text.encoding(v=vs.110).aspx
        /// </summary>
        public StringDeserializer()
        { }

        /// <summary>
        ///     Deserializes a string value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The data to deserialize.
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> deserialized to a string (or null if data is null).
        /// </returns>
        public string Deserialize(string topic, byte[] data)
        {
            if (data == null)
            {
                return null;
            }
            return encoding.GetString(data);
        }

        /// <include file='../include_docs.xml' path='API/Member[@name="IDeserializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var propertyName = isKey ? KeyEncodingConfigParam : ValueEncodingConfigParam;
            var keyOrValue = isKey ? "Key" : "Value";

            if (config.Any(ci => ci.Key == propertyName))
            {
                if (encoding != null)
                {
                    throw new ArgumentException($"{keyOrValue} StringDeserializer encoding was configured using both constructor and configuration parameter.");
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
                    throw new ArgumentException($"{keyOrValue} StringDeserializer encoding configuration parameter was specified twice.", e);
                }

                encoding = encodingName.ToEncoding();
                
                return config.Where(ci => ci.Key != propertyName);
            }

            if (encoding == null)
            {
                throw new ArgumentException($"{keyOrValue} StringDeserializer encoding was not configured.");
            }

            return config;
        }
    }
}
