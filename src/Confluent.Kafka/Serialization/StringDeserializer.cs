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

using System.Text;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A deserializer for string values.
    /// </summary>
    public class StringDeserializer : IDeserializer<string>
    {
        Encoding encoding;

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
        ///     Deserializes a string value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The data to deserialize.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> deserialized to a string (or null if data is null).
        /// </returns>
        public string Deserialize(byte[] data)
        {
            if (data == null)
            {
                return null;
            }
            return encoding.GetString(data);
        }
    }
}
