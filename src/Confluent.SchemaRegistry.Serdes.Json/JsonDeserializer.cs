// Copyright 2020-2023 Confluent Inc.
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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema;
using NJsonSchema.Validation;
#if NET8_0_OR_GREATER
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) JSON deserializer.
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           A magic byte that identifies this as a message with
    ///                         Confluent Platform framing.
    ///       bytes 1-4:        Unique global id of the JSON schema associated with
    ///                         the data (as registered in Confluent Schema Registry),
    ///                         big endian.
    ///       following bytes:  The JSON data (utf8)
    ///
    ///     Internally, uses Newtonsoft.Json for deserialization. Currently,
    ///     no explicit validation of the data is done against the
    ///     schema stored in Schema Registry.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the deserializer.
    /// </remarks>
    public class JsonDeserializer<T> : AsyncDeserializer<T, JsonSchema> where T : class
    {
        private readonly NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;

        private JsonSchemaValidator validator = new JsonSchemaValidator();

        private Schema schemaJson;

        private JsonSchema schema;

        private bool validate = true;

        private JsonSerializerSettings jsonSchemaGeneratorSettingsSerializerSettings {
            get =>
#if NET8_0_OR_GREATER
                            this.jsonSchemaGeneratorSettings?.SerializerSettings;
#else
                            this.jsonSchemaGeneratorSettings?.ActualSerializerSettings;
#endif
        }

        /// <summary>
        ///     Initialize a new JsonDeserializer instance.
        /// </summary>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonDeserializer(IEnumerable<KeyValuePair<string, string>> config = null, NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null) :
            this(null, config, jsonSchemaGeneratorSettings)
        {
        }

        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null, NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
            : this(schemaRegistryClient, config != null ? new JsonDeserializerConfig(config) : null, jsonSchemaGeneratorSettings)
        {
        }

        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, JsonDeserializerConfig config,
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null, RuleRegistry ruleRegistry = null)
            : base(schemaRegistryClient, config, ruleRegistry)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            if (config == null) { return; }

            var nonJsonConfig = config
                .Where(item => !item.Key.StartsWith("json.") && !item.Key.StartsWith("rules.")
                    && !item.Key.StartsWith("subject.name.strategy."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonDeserializer: unknown configuration parameter {nonJsonConfig.First().Key}.");
            }

            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToAsyncDelegate(schemaRegistryClient, config); }
            if (config.SchemaIdStrategy != null) { this.schemaIdDecoder = config.SchemaIdStrategy.Value.ToDeserializer(); }
            if (config.Validate != null) { this.validate = config.Validate.Value; }
        }

        /// <summary>
        ///     Initialize a new JsonDeserializer instance
        ///     with a given Schema.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance.
        /// </param>
        /// <param name="schema">
        ///     Schema to use for validation, used when external
        ///     schema references are present in the schema.
        ///     Populate the References list of the schema for
        ///     the same. Assuming the referenced schemas have
        ///     already been registered in the registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to
        ///     <see cref="JsonDeserializerConfig" />).
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonDeserializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, IEnumerable<KeyValuePair<string, string>> config = null,
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null) : this(schemaRegistryClient, config, jsonSchemaGeneratorSettings)
        {
            JsonSchemaResolver utils = new JsonSchemaResolver(
                schemaRegistryClient, schema, this.jsonSchemaGeneratorSettings);
            JsonSchema jsonSchema = utils.GetResolvedSchema().Result;
            this.schemaJson = schema;
            this.schema = jsonSchema;
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public override async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) { return null; }

            bool isKey = context.Component == MessageComponentType.Key;
            string topic = context.Topic;
            string subject = await GetSubjectName(topic, isKey, null).ConfigureAwait(false);
            RegisteredSchema latestSchema = null;
            if (subject != null)
            {
                latestSchema = await GetReaderSchema(subject)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }

            try
            {
                Schema writerSchemaJson = null;
                JsonSchema writerSchema = null;
                Schema readerSchemaJson;
                JsonSchema readerSchema;
                T value;
                IList<Migration> migrations = new List<Migration>();
                SchemaId writerId = new SchemaId(SchemaType.Json);
                var payload = schemaIdDecoder.Decode(data, context, ref writerId);
                
                if (schemaRegistryClient != null)
                {
                    (writerSchemaJson, writerSchema) = await GetWriterSchema(subject, writerId)
                        .ConfigureAwait(false);
                    if (subject == null)
                    {
                        subject = await GetSubjectName(topic, isKey, writerSchema.Title).ConfigureAwait(false);
                        if (subject != null)
                        {
                            latestSchema = await GetReaderSchema(subject)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }
                    }
                }
                payload = await ExecuteRules(context.Component == MessageComponentType.Key,
                        subject, context.Topic, context.Headers, RulePhase.Encoding, RuleMode.Read,
                        null, writerSchemaJson, payload, null)
                    .ContinueWith(t => t.Result is byte[] bytes ? new ReadOnlyMemory<byte>(bytes) : (ReadOnlyMemory<byte>)t.Result)
                    .ConfigureAwait(continueOnCapturedContext: false);

                if (latestSchema != null)
                {
                    migrations = await GetMigrations(subject, writerSchemaJson, latestSchema)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    readerSchemaJson = latestSchema;
                    readerSchema = await GetParsedSchema(latestSchema).ConfigureAwait(false);
                }
                else if (schema != null)
                {
                    readerSchemaJson = schemaJson;
                    readerSchema = schema;
                }
                else
                {
                    readerSchemaJson = writerSchemaJson;
                    readerSchema = writerSchema;
                }

                if (migrations.Count > 0)
                {
                    var serializedString = GetString(payload);
                    JToken json = Newtonsoft.Json.JsonConvert.DeserializeObject<JToken>(serializedString, jsonSchemaGeneratorSettingsSerializerSettings);
                    
                    json = await ExecuteMigrations(migrations, isKey, subject, topic, context.Headers, json)
                        .ContinueWith(t => (JToken)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    if (readerSchema != null && validate)
                    {
                        ICollection<ValidationError> validationResult;
                        lock (readerSchema)
                        {
                            validationResult = validator.Validate(json, readerSchema);
                        }
                        if (validationResult.Count > 0)
                        {
                            throw new InvalidDataException("Schema validation failed for properties: [" +
                                                           string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                        }
                    }

                    value = json.ToObject<T>(JsonSerializer.Create());
                }
                else
                {
                    var serializedString = GetString(payload);
                    if (readerSchema != null && validate)
                    {
                        ICollection<ValidationError> validationResult;
                        lock (readerSchema)
                        {
                            validationResult = validator.Validate(serializedString, readerSchema);
                        }

                        if (validationResult.Count > 0)
                        {
                            throw new InvalidDataException("Schema validation failed for properties: [" +
                                                           string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                        }
                    }

                    value = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(serializedString,
                        jsonSchemaGeneratorSettingsSerializerSettings);
                }

                if (readerSchema != null)
                {
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await JsonUtils.Transform(ctx, readerSchema, readerSchema, "$", message, transform).ConfigureAwait(false);
                    };
                    value = await ExecuteRules(context.Component == MessageComponentType.Key,
                            subject, context.Topic, context.Headers, RuleMode.Read,
                            null, readerSchemaJson, value, fieldTransformer)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                return value;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetString(ReadOnlyMemory<byte> data)
        {
#if NET8_0_OR_GREATER
            return Encoding.UTF8.GetString(data.Span);
#else

            var array = data.ToArray();
            return Encoding.UTF8.GetString(array);
#endif
        }

        protected override async Task<JsonSchema> ParseSchema(Schema schema)
        {
            JsonSchemaResolver utils = new JsonSchemaResolver(
                schemaRegistryClient, schema, jsonSchemaGeneratorSettings);
            
            return await utils.GetResolvedSchema().ConfigureAwait(false);
        }
    }
}
