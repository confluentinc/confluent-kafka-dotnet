// Copyright 2016-2018 Confluent Inc.
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
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    ///     A deserializer for use with <see cref="Confluent.Kafka.Consumer" />.
    /// </summary>
    /// <typeparam name="T">
    ///     The type the deserializer 
    /// </typeparam>
    public interface ITaskDeserializer<T>
    {
        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the message the raw data
        ///     is associated with.
        /// </param>
        /// <param name="data">
        ///     The data to deserialize.
        /// </param>
        /// <param name="isKey">
        ///     True if deserializing message key data, false if
        ///     deserializing message value data.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        Task<T> Deserialize(byte[] data, bool isKey, string topic);
    }
}