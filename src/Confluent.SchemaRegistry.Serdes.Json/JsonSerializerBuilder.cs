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
#if NET8_0_OR_GREATER
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     **EXPERIMENTAL**: subject to change or removal.
    ///
    ///     A builder of <see cref="JsonSerializer{T}" /> instances, for use with
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
    ///         .SetValueSerializerBuilder(new JsonSerializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryConfig(schemaRegistryConfig))
    ///         .Build();
    ///     </code>
    /// </example>
    public class JsonSerializerBuilder<T>
        : SchemaRegistrySerdeBuilder<JsonSerializerBuilder<T>>, IAsyncSerializerBuilder<T>
        where T : class
    {
        private JsonSerializerConfig serializerConfig;
        private Func<JsonSerializer<T>, Task> serializerInit;
        private NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private Schema schema;

        /// <summary>
        ///     The serializer configuration (refer to
        ///     <see cref="JsonSerializerConfig" />).
        /// </summary>
        public JsonSerializerBuilder<T> SetSerializerConfig(JsonSerializerConfig serializerConfig)
        {
            this.serializerConfig = serializerConfig;
            return this;
        }

        /// <summary>
        ///     The JSON schema generator settings to use when deriving the schema
        ///     from <typeparamref name="T" />.
        /// </summary>
        public JsonSerializerBuilder<T> SetJsonSchemaGeneratorSettings(
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
            return this;
        }

        /// <summary>
        ///     An explicit schema to use, rather than deriving one from
        ///     <typeparamref name="T" />. Required when the schema has references.
        /// </summary>
        public JsonSerializerBuilder<T> SetSchema(Schema schema)
        {
            this.schema = schema;
            return this;
        }

        /// <summary>
        ///     **EXPERIMENTAL**: subject to change or removal.
        ///
        ///     Setup to run on the serializer once it has been built, for anything
        ///     the other setters do not cover. It runs to completion within
        ///     <c>Build</c>; if it throws, the serializer's owned resources are
        ///     released and the exception propagates.
        ///
        ///     Do not serialize from it. It runs while the producer is still
        ///     being constructed, before the serializer has been handed the
        ///     resolver for the Kafka cluster id, so a subject name strategy that
        ///     depends on that id - such as <see cref="AssociatedNameStrategy" /> - would
        ///     resolve, and cache, the wrong subject. To warm the serializer up,
        ///     serialize once the client has been constructed, or supply a serializer
        ///     constructed directly with
        ///     <see cref="AssociatedNameStrategy.KafkaClusterIdConfig" /> configured
        ///     instead of using the builder.
        /// </summary>
        public JsonSerializerBuilder<T> SetSerializerInit(Func<JsonSerializer<T>, Task> serializerInit)
        {
            this.serializerInit = serializerInit;
            return this;
        }

        /// <inheritdoc />
        public IAsyncSerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
            => ConstructSerde(
                client => schema == null
                    ? new JsonSerializer<T>(
                        client, serializerConfig, jsonSchemaGeneratorSettings, ruleRegistry)
                    : new JsonSerializer<T>(
                        client, schema, serializerConfig, jsonSchemaGeneratorSettings, ruleRegistry),
                serializer => serializer.OwnSchemaRegistryClient(),
                serializerInit);
    }
}
