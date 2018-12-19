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
    ///     A deserializer for use with <see cref="Confluent.Kafka.Consumer{TKey,TValue}" />.
    /// </summary>
    public interface IAsyncDeserializer<T>
    {
        /// <summary>
        ///     Deserialize a message key or value.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="messageMetadata">
        ///     Properties of the message the data is associated with
        ///     extra to the key or value.
        /// </param>
        /// <param name="source">
        ///     The TopicPartition from which the message was consumed.
        /// </param>
        /// <param name="isKey">
        ///     True if deserializing the message key, false if deserializing the
        ///     message value.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, bool isKey, MessageMetadata messageMetadata, TopicPartition source);
    }
}
