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
using Google.Protobuf;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     A builder of <see cref="ProtobufDeserializer{T}" /> instances, for use with
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
    ///         .SetValueDeserializerBuilder(new ProtobufDeserializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryConfig(schemaRegistryConfig))
    ///         .Build();
    ///     </code>
    /// </example>
    public class ProtobufDeserializerBuilder<T>
        : SchemaRegistrySerdeBuilder<ProtobufDeserializerBuilder<T>>, IAsyncDeserializerBuilder<T>
        where T : class, IMessage<T>, new()
    {
        private ProtobufDeserializerConfig deserializerConfig;

        /// <summary>
        ///     Protobuf messages are deserialized into the generated type, so no
        ///     schema needs to be looked up and a Schema Registry client is optional.
        /// </summary>
        protected override bool RequiresSchemaRegistryClient
            => false;

        /// <summary>
        ///     The deserializer configuration (refer to
        ///     <see cref="ProtobufDeserializerConfig" />).
        /// </summary>
        public ProtobufDeserializerBuilder<T> SetDeserializerConfig(
            ProtobufDeserializerConfig deserializerConfig)
        {
            this.deserializerConfig = deserializerConfig;
            return this;
        }

        /// <inheritdoc />
        public IAsyncDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            var client = ResolveSchemaRegistryClient(out bool owned);
            var deserializer = new ProtobufDeserializer<T>(client, deserializerConfig, ruleRegistry);

            if (owned)
            {
                deserializer.OwnSchemaRegistryClient();
            }

            return deserializer;
        }
    }
}
