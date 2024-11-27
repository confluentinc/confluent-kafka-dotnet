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
using System.Collections;
using System.Collections.Generic;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A collection of Kafka message headers.
    /// </summary>
    /// <remarks>
    ///     Message headers are supported by v0.11 brokers and above.
    /// </remarks>
    public class Headers : IEnumerable<IHeader>
    {
        /// <summary>
        /// Backing list is only created on first actual header
        /// </summary>
        private List<IHeader> headers = null;

        /// <summary>
        /// Gets the underlying list of headers
        /// </summary>
        public IReadOnlyList<IHeader> BackingList => (IReadOnlyList<IHeader>)headers ?? Array.Empty<IHeader>();

        /// <summary>
        ///     Append a new header to the collection.
        /// </summary>
        /// <param name="key">
        ///     The header key.
        /// </param>
        /// <param name="val">
        ///     The header value (possibly null). Note: A null
        ///     header value is distinct from an empty header
        ///     value (array of length 0).
        /// </param>
        public void Add(string key, byte[] val)
        {
            if (key == null) 
                throw new ArgumentNullException(nameof(key), "Kafka message header key cannot be null.");

            headers ??= new();
            headers.Add(new Header(key, val));
        }

        /// <summary>
        ///     Append a new header to the collection.
        /// </summary>
        /// <param name="header">
        ///     The header to add to the collection.
        /// </param>
        public void Add(Header header)
        {
            headers ??= new();
            headers.Add(header);
        }

        /// <summary>
        ///     Get the value of the latest header with the specified key.
        /// </summary>
        /// <param name="key">
        ///     The key to get the associated value of.
        /// </param>
        /// <returns>
        ///     The value of the latest element in the collection with the specified key.
        /// </returns>
        /// <exception cref="System.Collections.Generic.KeyNotFoundException">
        ///     The key <paramref name="key" /> was not present in the collection.
        /// </exception>
        public byte[] GetLastBytes(string key)
        {
            if (TryGetLastBytes(key, out byte[] result))
            {
                return result;
            }

            throw new KeyNotFoundException($"The key {key} was not present in the headers collection.");
        }

        /// <summary>
        ///     Try to get the value of the latest header with the specified key.
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
        public bool TryGetLastBytes(string key, out byte[] lastHeader)
        {
            if (headers != null)
                for (int i = headers.Count - 1; i >= 0; --i)
                {
                    if (headers[i].Key == key)
                    {
                        lastHeader = headers[i].GetValueBytes();
                        return true;
                    }
                }

            lastHeader = default;
            return false;
        }


        /// <summary>
        ///     Removes all headers for the given key.
        /// </summary>
        /// <param name="key">
        ///     The key to remove all headers for
        /// </param>
        public void Remove(string key) => headers?.RemoveAll(a => a.Key == key);

        /// <summary>
        /// Removes all headers from the collection.
        /// </summary>
        public void Clear() => headers?.Clear();

        /// <summary>
        ///     Returns an enumerator that iterates through the headers collection.
        /// </summary>
        /// <returns>
        ///     An enumerator object that can be used to iterate through the headers collection.
        /// </returns>
        public IEnumerator<IHeader> GetEnumerator() => headers?.GetEnumerator() ?? Enumerable.Empty<IHeader>().GetEnumerator();

        /// <summary>
        ///     Returns an enumerator that iterates through the headers collection.
        /// </summary>
        /// <returns>
        ///     An enumerator object that can be used to iterate through the headers collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        ///     Gets the header at the specified index
        /// </summary>
        /// <param key="index">
        ///     The zero-based index of the element to get.
        /// </param>
        public IHeader this[int index] => headers?[index] ?? throw new ArgumentOutOfRangeException(nameof(index), "Header collection is empty.");

        /// <summary>
        ///     The number of headers in the collection.
        /// </summary>
        public int Count => headers?.Count ?? 0;
    }
}
