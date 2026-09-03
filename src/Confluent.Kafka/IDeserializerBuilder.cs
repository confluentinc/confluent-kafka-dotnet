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
    ///     Defines a builder of <see cref="IDeserializer{T}" /> instances, for use with
    ///     <see cref="ConsumerBuilder{TKey,TValue}.SetKeyDeserializerBuilder(IDeserializerBuilder{TKey})" />
    ///     and its value counterpart.
    ///
    ///     In contrast to supplying an already constructed deserializer, a builder is
    ///     invoked by the consumer during construction, and is passed the consumer's
    ///     own configuration along with whether the deserializer is being used for the
    ///     message key or value.
    /// </summary>
    public interface IDeserializerBuilder<T>
    {
        /// <summary>
        ///     Build a deserializer.
        /// </summary>
        /// <param name="config">
        ///     The configuration of the consumer the deserializer will be used by.
        /// </param>
        /// <param name="isKey">
        ///     Whether the deserializer will be used to deserialize message keys
        ///     (true) or message values (false).
        /// </param>
        /// <returns>
        ///     The deserializer.
        /// </returns>
        IDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey);
    }
}
