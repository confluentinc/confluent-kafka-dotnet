// Copyright 2018 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a kafka message header.
    /// </summary>
    /// <remarks>
    ///     Message headers are supported by v0.11 brokers and above.
    /// </remarks>
    public class Header : IHeader
    {
        private byte[] val;

        /// <summary>
        ///     The header key.
        /// </summary>
        public string Key { get; private set; }

        /// <summary>
        ///     Get the serialized header value data.
        /// </summary>
        public byte[] GetValueBytes()
        {
            return val;
        }
        
        /// <summary>
        ///     Create a new Header instance.
        /// </summary>
        /// <param name="key">
        ///     The header key.
        /// </param>
        /// <param name="value">
        ///     The header value (may be null).
        /// </param>
        public Header(string key, byte[] value)
        {
            if (key == null) 
            {
                throw new ArgumentNullException("Kafka message header key cannot be null.");
            }

            Key = key;
            val = value;
        }
    }
}
