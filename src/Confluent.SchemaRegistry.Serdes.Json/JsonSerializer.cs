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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using NJsonSchema;
using Newtonsoft.Json;
using NJsonSchema.Validation;
#if NET8_0_OR_GREATER
using NJsonSchema.NewtonsoftJson.Generation;
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.NewtonsoftJson.Generation.NewtonsoftJsonSchemaGeneratorSettings;
#else
using NewtonsoftJsonSchemaGeneratorSettings = NJsonSchema.Generation.JsonSchemaGeneratorSettings;
#endif
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     JSON Serializer.
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
    ///     Internally, the serializer uses Newtonsoft.Json for
    ///     serialization and NJsonSchema for schema creation and
    ///     validation. You can use any property annotations recognised
    ///     by these libraries.
    ///
    ///     Note: Off-the-shelf libraries do not yet exist to enable
    ///     integration of System.Text.Json and JSON Schema, so this
    ///     is not yet supported by the serializer.
    /// </remarks>
    public class JsonSerializer<T> : AsyncSerializer<T, JsonSchema> where T : class
    {
        private readonly NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;
        private readonly List<SchemaReference> ReferenceList = new List<SchemaReference>();
        
        private JsonSchemaValidator validator = new JsonSchemaValidator();

        /// <remarks>
        ///     A given schema is uniquely identified by a schema id, even when
        ///     registered against multiple subjects.
        /// </remarks>
        private int? schemaId;

        private JsonSchema schema;
        private string schemaText;
        private string schemaFullname;

        private JsonSerializerSettings jsonSchemaGeneratorSettingsSerializerSettings {
            get =>
#if NET8_0_OR_GREATER
                            this.jsonSchemaGeneratorSettings?.SerializerSettings;
#else
                            this.jsonSchemaGeneratorSettings?.ActualSerializerSettings;
#endif
        }

        /// <summary>
        ///     Initialize a new instance of the JsonSerializer class.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance.
        /// </param>
        /// <param name="config">
        ///     Serializer configuration.
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerConfig config = null, 
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null, RuleRegistry ruleRegistry = null)
            : base(schemaRegistryClient, config, ruleRegistry)
        {
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            this.schema = this.jsonSchemaGeneratorSettings == null
#if NET8_0_OR_GREATER
                ? NewtonsoftJsonSchemaGenerator.FromType<T>()
                : NewtonsoftJsonSchemaGenerator.FromType<T>(this.jsonSchemaGeneratorSettings);
#else
                ? JsonSchema.FromType<T>()
                : JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
#endif
            this.schemaText = schema.ToJson();
            this.schemaFullname = schema.Title;
            
            if (config == null) { return; }

            var nonJsonConfig = config
                .Where(item => !item.Key.StartsWith("json.") && !item.Key.StartsWith("rules."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonSerializer: unknown configuration parameter {nonJsonConfig.First().Key}");
            }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.LatestCompatibilityStrict != null) { this.latestCompatibilityStrict = config.LatestCompatibilityStrict.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"JsonSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        /// <summary>
        ///     Initialize a new instance of the JsonSerializer class
        ///     with a given Schema.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     Confluent Schema Registry client instance.
        /// </param>
        /// <param name="schema">
        ///     Schema to use for validation, used when external
        ///     schema references are present in the schema. 
        ///     Populate the References list of the schema for
        ///     the same.
        /// </param>
        /// <param name="config">
        ///     Serializer configuration.
        /// </param>
        /// <param name="jsonSchemaGeneratorSettings">
        ///     JSON schema generator settings.
        /// </param>
        public JsonSerializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSerializerConfig config = null, 
            NewtonsoftJsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null, RuleRegistry ruleRegistry = null)
            : this(schemaRegistryClient, config, jsonSchemaGeneratorSettings, ruleRegistry)
        {
            foreach (var reference in schema.References)
            {
                ReferenceList.Add(reference);
            }

            JsonSchemaResolver utils = new JsonSchemaResolver(
                schemaRegistryClient, schema, this.jsonSchemaGeneratorSettings);
            JsonSchema jsonSchema = utils.GetResolvedSchema().Result;
            this.schema = jsonSchema;
            this.schemaText = schema.SchemaString;
            this.schemaFullname = jsonSchema.Title;
        }

        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a UTF8 encoded JSON 
        ///     representation. The serialized data is preceeded by:
        ///       1. A "magic byte" (1 byte) that identifies this as a message with
        ///          Confluent Platform framing.
        ///       2. The id of the schema as registered in Confluent's Schema Registry
        ///          (4 bytes, network byte order).
        ///     This call may block or throw on first use for a particular topic during
        ///     schema registration / verification.
        /// </summary>
        /// <param name="value">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes with 
        ///     <paramref name="value" /> serialized as a byte array.
        /// </returns>
        public override async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            if (value == null) { return null; }

            try
            {
                string subject;
                RegisteredSchema latestSchema = null;
                await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    subject = GetSubjectName(context.Topic, context.Component == MessageComponentType.Key, this.schemaFullname);
                    latestSchema = await GetReaderSchema(subject, new Schema(schemaText, ReferenceList, SchemaType.Json))
                        .ConfigureAwait(continueOnCapturedContext: false);
                    
                    if (latestSchema != null)
                    {
                        schemaId = latestSchema.Id;
                    }
                    else if (!subjectsRegistered.Contains(subject))
                    {
                        // first usage: register/get schema to check compatibility
                        schemaId = autoRegisterSchema
                            ? await schemaRegistryClient.RegisterSchemaAsync(subject,
                                    new Schema(this.schemaText, ReferenceList, SchemaType.Json), normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false)
                            : await schemaRegistryClient.GetSchemaIdAsync(subject,
                                    new Schema(this.schemaText, ReferenceList, SchemaType.Json), normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false);

                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serdeMutex.Release();
                }
                
                if (latestSchema != null)
                {
                    var latestSchemaJson = await GetParsedSchema(latestSchema).ConfigureAwait(false);
                    FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                    {
                        return await JsonUtils.Transform(ctx, latestSchemaJson, "$", message, transform).ConfigureAwait(false);
                    };
                    value = await ExecuteRules(context.Component == MessageComponentType.Key, subject,
                            context.Topic, context.Headers, RuleMode.Write, null,
                            latestSchema, value, fieldTransformer)
                        .ContinueWith(t => (T)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                var serializedString = Newtonsoft.Json.JsonConvert.SerializeObject(value, jsonSchemaGeneratorSettingsSerializerSettings);
                var validationResult = validator.Validate(serializedString, this.schema);
                if (validationResult.Count > 0)
                {
                    throw new InvalidDataException("Schema validation failed for properties: [" + string.Join(", ", validationResult.Select(r => r.Path)) + "]");
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(schemaId.Value));
                    writer.Write(System.Text.Encoding.UTF8.GetBytes(serializedString));
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected override async Task<JsonSchema> ParseSchema(Schema schema)
        {
            JsonSchemaResolver utils = new JsonSchemaResolver(
                schemaRegistryClient, schema, jsonSchemaGeneratorSettings);
            return await utils.GetResolvedSchema();
        }
    }
}
