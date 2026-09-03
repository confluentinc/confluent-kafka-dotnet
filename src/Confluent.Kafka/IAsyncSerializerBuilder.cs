// Copyright 2025 Confluent Inc.
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

using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a builder of <see cref="IAsyncSerializer{T}" /> instances, for use with
    ///     <see cref="ProducerBuilder{TKey,TValue}.SetKeySerializerBuilder(IAsyncSerializerBuilder{TKey})" />
    ///     and its value counterpart.
    ///
    ///     In contrast to supplying an already constructed serializer, a builder is
    ///     invoked by the producer during construction, and is passed the producer's
    ///     own configuration along with whether the serializer is being used for the
    ///     message key or value.
    /// </summary>
    public interface IAsyncSerializerBuilder<T>
    {
        /// <summary>
        ///     Build a serializer.
        /// </summary>
        /// <param name="config">
        ///     The configuration of the producer the serializer will be used by.
        /// </param>
        /// <param name="isKey">
        ///     Whether the serializer will be used to serialize message keys
        ///     (true) or message values (false).
        /// </param>
        /// <returns>
        ///     The serializer.
        /// </returns>
        IAsyncSerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey);
    }
}
