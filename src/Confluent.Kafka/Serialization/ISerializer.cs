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
using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Implement this interface to define a serializer 
    ///     for a particular type T.
    /// </summary>
    public interface ISerializer<T>
    {
        /// <summary>
        ///     Serialize an instance of type T to a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated wih the data.
        /// </param>
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <param name="isKey">
        ///     true: deserialization is for a key, 
        ///     false: deserializing is for a value.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        byte[] Serialize(string topic, T data, bool isKey);

        /// <summary>
        ///     Configuration properties used by the serializer.
        /// </summary>
        IEnumerable<KeyValuePair<string, object>> Configuration { get; }
    }
}
