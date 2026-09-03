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
#if NET8_0_OR_GREATER
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
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

        /// <inheritdoc />
        public IAsyncSerializer<T> Build(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            var client = ResolveSchemaRegistryClient(out bool owned);

            var serializer = schema == null
                ? new JsonSerializer<T>(
                    client, serializerConfig, jsonSchemaGeneratorSettings, ruleRegistry)
                : new JsonSerializer<T>(
                    client, schema, serializerConfig, jsonSchemaGeneratorSettings, ruleRegistry);

            if (owned)
            {
                serializer.OwnSchemaRegistryClient();
            }

            return serializer;
        }
    }
}
