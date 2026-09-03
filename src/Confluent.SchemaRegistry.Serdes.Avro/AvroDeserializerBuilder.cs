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
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
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

        /// <summary>
        ///     The deserializer configuration (refer to
        ///     <see cref="AvroDeserializerConfig" />).
        /// </summary>
        public AvroDeserializerBuilder<T> SetDeserializerConfig(AvroDeserializerConfig deserializerConfig)
        {
            this.deserializerConfig = deserializerConfig;
            return this;
        }

        /// <inheritdoc />
        public IAsyncDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            var client = ResolveSchemaRegistryClient(out bool owned);
            var deserializer = new AvroDeserializer<T>(client, deserializerConfig, ruleRegistry);

            if (owned)
            {
                deserializer.OwnSchemaRegistryClient();
            }

            return deserializer;
        }
    }
}
