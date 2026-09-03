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
    ///     A builder of <see cref="AvroSerializer{T}" /> instances, for use with
    ///     <see cref="ProducerBuilder{TKey,TValue}.SetValueSerializerBuilder(IAsyncSerializerBuilder{TValue})" />
    ///     and its key counterpart.
    ///
    ///     The producer constructs the serializer during its own construction,
    ///     supplying its configuration and whether the serializer is for the message
    ///     key or value, and owns the result.
    /// </summary>
    /// <example>
    ///     <code>
    ///     using var producer = new ProducerBuilder&lt;string, User&gt;(producerConfig)
    ///         .SetValueSerializerBuilder(new AvroSerializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryConfig(schemaRegistryConfig))
    ///         .Build();
    ///     </code>
    /// </example>
    public class AvroSerializerBuilder<T>
        : SchemaRegistrySerdeBuilder<AvroSerializerBuilder<T>>, IAsyncSerializerBuilder<T>
    {
        private AvroSerializerConfig serializerConfig;

        /// <summary>
        ///     The serializer configuration (refer to
        ///     <see cref="AvroSerializerConfig" />).
        /// </summary>
        public AvroSerializerBuilder<T> SetSerializerConfig(AvroSerializerConfig serializerConfig)
        {
            this.serializerConfig = serializerConfig;
            return this;
        }

        /// <inheritdoc />
        public IAsyncSerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            var client = ResolveSchemaRegistryClient(out bool owned);
            var serializer = new AvroSerializer<T>(client, serializerConfig, ruleRegistry);

            if (owned)
            {
                serializer.OwnSchemaRegistryClient();
            }

            return serializer;
        }
    }
}
