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
    public class StringSerializer : ISerializer<string>
    {
        private Encoding encoding;

        public StringSerializer(Encoding encoding)
        {
            this.encoding = encoding;
        }

        /// <param name="val">
        ///     The string value to serialize.
        /// </param>
        /// <returns>
        ///     <paramref name="val" /> encoded in a byte array (or null if <paramref name="val" /> is null).
        /// </returns>
        public byte[] Serialize(string val)
        {
            if (val == null)
            {
                return null;
            }
            return encoding.GetBytes(val);
        }
    }
}
