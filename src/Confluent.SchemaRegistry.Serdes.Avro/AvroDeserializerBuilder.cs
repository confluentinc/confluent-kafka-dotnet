// Copyright 2026 Confluent Inc.
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
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     **EXPERIMENTAL**: subject to change or removal.
    ///
    ///     A builder of <see cref="AvroDeserializer{T}" /> instances, for use with
    ///     <see cref="ConsumerBuilder{TKey,TValue}.SetValueDeserializerBuilder(IAsyncDeserializerBuilder{TValue})" />
    ///     and its key counterpart.
    ///
    ///     The consumer constructs the deserializer during its own construction,
    ///     supplying its configuration and whether the deserializer is for the
    ///     message key or value, and owns the result. There is no need to adapt the
    ///     deserializer with <c>AsSyncOverAsync()</c> - the consumer does so itself.
    /// </summary>
    /// <example>
    ///     <code>
    ///     using var consumer = new ConsumerBuilder&lt;string, User&gt;(consumerConfig)
    ///         .SetValueDeserializerBuilder(new AvroDeserializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryConfig(schemaRegistryConfig))
    ///         .Build();
    ///     </code>
    /// </example>
    public class AvroDeserializerBuilder<T>
        : SchemaRegistrySerdeBuilder<AvroDeserializerBuilder<T>>, IAsyncDeserializerBuilder<T>
    {
        private AvroDeserializerConfig deserializerConfig;
        private Func<AvroDeserializer<T>, Task> deserializerInit;

        /// <summary>
        ///     The deserializer configuration (refer to
        ///     <see cref="AvroDeserializerConfig" />).
        /// </summary>
        public AvroDeserializerBuilder<T> SetDeserializerConfig(AvroDeserializerConfig deserializerConfig)
        {
            this.deserializerConfig = deserializerConfig;
            return this;
        }

        /// <summary>
        ///     **EXPERIMENTAL**: subject to change or removal.
        ///
        ///     Setup to run on the deserializer once it has been built, for anything
        ///     the other setters do not cover. It runs to completion within
        ///     <c>Build</c>; if it throws, the deserializer's owned resources are
        ///     released and the exception propagates.
        ///
        ///     Do not deserialize from it. It runs while the consumer is still
        ///     being constructed, before the deserializer has been handed the
        ///     resolver for the Kafka cluster id, so a subject name strategy that
        ///     depends on that id - such as <see cref="AssociatedNameStrategy" /> - would
        ///     resolve, and cache, the wrong subject. To warm the deserializer up,
        ///     deserialize once the client has been constructed, or supply a deserializer
        ///     constructed directly with
        ///     <see cref="AssociatedNameStrategy.KafkaClusterIdConfig" /> configured
        ///     instead of using the builder.
        /// </summary>
        public AvroDeserializerBuilder<T> SetDeserializerInit(Func<AvroDeserializer<T>, Task> deserializerInit)
        {
            this.deserializerInit = deserializerInit;
            return this;
        }

        /// <inheritdoc />
        public IAsyncDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
            => ConstructSerde(
                client => new AvroDeserializer<T>(client, deserializerConfig, ruleRegistry),
                deserializer => deserializer.OwnSchemaRegistryClient(),
                deserializerInit);
    }
}
