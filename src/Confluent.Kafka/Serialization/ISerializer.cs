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
    public interface ISerializer<T> : IDisposable
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
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        byte[] Serialize(string topic, T data);


        /// <summary>
        ///     Configure the serializer using relevant configuration parameter(s) in <paramref name="config" /> (if present)
        /// </summary>
        /// <param name="config">
        ///     A collection containing configuration parameter(s) relevant to this serializer.
        /// </param>
        /// <param name="isKey">
        ///     true: if this serializer instance is used to serialize keys,
        ///     false: if this serializer instance is used to serialize values.
        /// </param>
        /// <returns>
        ///     A configuration collection with configuration parameter(s) relevant to this serializer removed.
        /// </returns>
        IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey);
    }
}
