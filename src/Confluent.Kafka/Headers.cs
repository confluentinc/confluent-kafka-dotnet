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
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A collection of Kafka message headers.
    /// </summary>
    public class Headers : List<KeyValuePair<string, byte[]>>
    {
        /// <summary>
        ///     Append a new header to the collection.
        /// </summary>
        /// <param name="key">
        ///     The header key.
        /// </param>
        /// <param name="val">
        ///     The header value.
        /// </param>
        public void Add(string key, byte[] val)
        {
            if (key == null) 
            {
                throw new ArgumentNullException("Kafka message header key cannot be null.");
            }

            Add(new KeyValuePair<string, byte[]>(key, val));
        }


        /// <summary>
        ///     Get the value of the latest message with the specified key.
        /// </summary>
        /// <param name="key">
        ///     The key to get the associated value of.
        /// </param>
        /// <returns>
        ///     The value of the latest element in the collection with the specified key.
        /// </returns>
        /// <exception cref="KeyNotFoundException">
        ///     The key <paramref name="key" /> was not present in the collection.
        /// </exception>
        public byte[] GetLast(string key)
        {
            if (TryGetLast(key, out byte[] result))
            {
                return result;
            }

            throw new KeyNotFoundException($"The key {key} was not present in the headers collection.");
        }

        /// <summary>
        ///     Try to get the value of the latest message with the specified key.
        /// </summary>
        /// <param name="key">
        ///     The key to get the associated value of.
        /// </param>
        /// <param name="lastHeader">
        ///     The value of the latest element in the collection with the 
        ///     specified key, if a header with that key was present in the
        ///     collection.
        /// </param>
        /// <returns>
        ///     true if the a value with the specified key was present in 
        ///     the collection, false otherwise.
        /// </returns>
        public bool TryGetLast(string key, out byte[] lastHeader)
        {
            for (int i=this.Count-1; i>=0; --i)
            {
                if (this[i].Key == key)
                {
                    lastHeader = this[i].Value;
                    return true;
                }
            }

            lastHeader = null;
            return false;
        }
    }
}
