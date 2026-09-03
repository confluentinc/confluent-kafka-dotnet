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

using System;
using System.Collections.Generic;
using Confluent.Kafka;
#if NET8_0_OR_GREATER
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     A builder of <see cref="JsonDeserializer{T}" /> instances, for use with
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
    ///         .SetValueDeserializerBuilder(new JsonDeserializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryConfig(schemaRegistryConfig))
    ///         .Build();
    ///     </code>
    /// </example>
    public class JsonDeserializerBuilder<T>
        : SchemaRegistrySerdeBuilder<JsonDeserializerBuilder<T>>, IAsyncDeserializerBuilder<T>
        where T : class
    {
        private JsonDeserializerConfig deserializerConfig;

        /// <summary>
        ///     A JSON schema can be derived from the target type, so a Schema
        ///     Registry client is optional unless an explicit schema is used.
        /// </summary>
        protected override bool RequiresSchemaRegistryClient
            => schema != null;
        private NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private Schema schema;

        /// <summary>
        ///     The deserializer configuration (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </summary>
        public JsonDeserializerBuilder<T> SetDeserializerConfig(JsonDeserializerConfig deserializerConfig)
        {
            this.deserializerConfig = deserializerConfig;
            return this;
        }

        /// <summary>
        ///     The JSON schema generator settings to use when resolving the schema.
        /// </summary>
        public JsonDeserializerBuilder<T> SetJsonSchemaGeneratorSettings(
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
            return this;
        }

        /// <summary>
        ///     An explicit schema to validate against. Required when the schema has
        ///     references.
        /// </summary>
        public JsonDeserializerBuilder<T> SetSchema(Schema schema)
        {
            this.schema = schema;
            return this;
        }

        /// <inheritdoc />
        public IAsyncDeserializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            // JsonDeserializer has no constructor accepting both an explicit schema
            // and a rule registry, so reject the combination rather than silently
            // dropping the rule registry.
            if (schema != null && ruleRegistry != null)
            {
                throw new ArgumentException(
                    "JsonDeserializer does not support specifying both an explicit schema and a rule registry.");
            }

            var client = ResolveSchemaRegistryClient(out bool owned);

            var deserializer = schema == null
                ? new JsonDeserializer<T>(
                    client, deserializerConfig, jsonSchemaGeneratorSettings, ruleRegistry)
                : new JsonDeserializer<T>(
                    client, schema, deserializerConfig, jsonSchemaGeneratorSettings);

            if (owned)
            {
                deserializer.OwnSchemaRegistryClient();
            }

            return deserializer;
        }
    }
}
