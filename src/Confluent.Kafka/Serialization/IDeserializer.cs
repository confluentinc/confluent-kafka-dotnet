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
    ///     Implement this interface to define a deserializer 
    ///     for a particular type T.
    /// </summary>
    public interface IDeserializer<T> : IDisposable
    {
        /// <summary>
        ///     Deserialize a byte array to an instance of
        ///     type T.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated wih the data.
        /// </param>
        /// <param name="data">
        ///     The serialized representation of an instance
        ///     of type T to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if data is null, false otherwise. If true,
        ///     ReadOnlySpan reference is not valid.
        /// </param>
        /// <returns>
        ///     The deserialized value.
        /// </returns>
        T Deserialize(string topic, ReadOnlySpan<byte> data, bool isNull);


        /// <summary>
        ///     Configure the deserializer using relevant configuration parameter(s) in <paramref name="config" /> (if present).
        /// </summary>
        /// <param name="config">
        ///     A collection containing configuration parameter(s) relevant to this deserializer.
        /// </param>
        /// <param name="isKey">
        ///     true: if this deserializer instance is used to serialize keys,
        ///     false: if this deserializer instance is used to serialize values.
        /// </param>
        /// <returns>
        ///     A configuration collection with configuration parameter(s) relevant to this deserializer removed.
        /// </returns>
        IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey);
    }
}
