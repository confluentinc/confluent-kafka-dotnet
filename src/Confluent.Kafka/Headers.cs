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
        private readonly List<IHeader> headers = new List<IHeader>();

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
            {
                throw new ArgumentNullException("Kafka message header key cannot be null.");
            }

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
        /// <exception cref="KeyNotFoundException">
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
            for (int i=headers.Count-1; i>=0; --i)
            {
                if (headers[i].Key == key)
                {
                    lastHeader = headers[i].GetValueBytes();
                    return true;
                }
            }

            lastHeader = default(byte[]);
            return false;
        }


        /// <summary>
        ///     Removes all headers for the given key.
        /// </summary>
        /// <param name="key">
        ///     The key to remove all headers for
        /// </param>
        public void Remove(string key)
            => headers.RemoveAll(a => a.Key == key);

        internal class HeadersEnumerator : IEnumerator<IHeader>
        {
            private Headers headers;

            private int location = -1;

            public HeadersEnumerator(Headers headers)
            {
                this.headers = headers;
            }

            public object Current 
                => ((IEnumerator<IHeader>)this).Current;

            IHeader IEnumerator<IHeader>.Current
                => headers.headers[location];

            public void Dispose() {}

            public bool MoveNext()
            {
                location += 1;
                if (location >= headers.headers.Count)
                {
                    return false;
                }

                return true;
            }

            public void Reset()
            {
                this.location = -1;
            }
        }

        /// <summary>
        ///     Returns an enumerator that iterates through the headers collection.
        /// </summary>
        /// <returns>
        ///     An enumerator object that can be used to iterate through the headers collection.
        /// </returns>
        public IEnumerator<IHeader> GetEnumerator()
            => new HeadersEnumerator(this);

        /// <summary>
        ///     Returns an enumerator that iterates through the headers collection.
        /// </summary>
        /// <returns>
        ///     An enumerator object that can be used to iterate through the headers collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
            => new HeadersEnumerator(this);

        /// <summary>
        ///     Gets the header at the specified index
        /// </summary>
        /// <param key="index">
        ///     The zero-based index of the element to get.
        /// </param>
        public IHeader this[int index]
            => headers[index];

        /// <summary>
        ///     The number of headers in the collection.
        /// </summary>
        public int Count
            => headers.Count;
    }
}
