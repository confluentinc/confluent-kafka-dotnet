// Copyright 2020 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using System.Reflection.Emit;
using NJsonSchema;
using NJsonSchema.Generation;
using NJsonSchema.Validation;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
    public class JsonSerializer<T> : IAsyncSerializer<T> where T : class
    {
        private const int DefaultInitialBufferSize = 1024;

        private bool autoRegisterSchema = true;
        private bool normalizeSchemas = false;
        private bool useLatestVersion = false;
        private bool latestCompatibilityStrict = false;
        private int initialBufferSize = DefaultInitialBufferSize;
        private SubjectNameStrategyDelegate subjectNameStrategy = null;
        private ISchemaRegistryClient schemaRegistryClient;
        private readonly JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings;

        private HashSet<string> subjectsRegistered = new HashSet<string>();
        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);
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
        private Dictionary<string, Schema> dict_schema_name_to_schema = new Dictionary<string, Schema>();
        private Dictionary<string, JsonSchema> dict_schema_name_to_JsonSchema = new Dictionary<string, JsonSchema>();
        private static int curRefNo = 0;
        private static Type GetTypeForSchema()
        {
            string className = "S" + curRefNo.ToString();
            curRefNo++;
            string nameSpace = "Confluent.SchemaRegistry.Serdes";
            AssemblyName assemblyName = new AssemblyName(nameSpace);
            System.Reflection.Emit.AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndCollect);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(nameSpace);
            TypeBuilder typeBuilder = moduleBuilder.DefineType(nameSpace + "." + className, TypeAttributes.Public);
            Type dynamicType = typeBuilder.CreateTypeInfo().AsType();
            return dynamicType;
        }
        private void create_schema_dict_util(Schema root)
        {
            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            this.dict_schema_name_to_schema.Add(schemaId, root);

            foreach (var reference in root.References)
            {
                Schema ref_schema_res = this.schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version).Result;
                create_schema_dict_util(ref_schema_res);
            }
        }

        private JsonSchema getSchemaUtil(Schema root)
        {
            List<SchemaReference> refers = root.References;
            foreach (var x in refers)
            {
                dict_schema_name_to_JsonSchema.Add(
                    x.Name, getSchemaUtil(dict_schema_name_to_schema[x.Name]));
            }

            Func<JsonSchema, JsonReferenceResolver> factory;
            factory = x =>
            {
                JsonSchemaResolver schemaResolver =
                    new JsonSchemaResolver(x, new JsonSchemaGeneratorSettings());
                foreach (var reference in refers)
                {
                    JsonSchema jschema =
                        dict_schema_name_to_JsonSchema[reference.Name];
                    Type type = GetTypeForSchema();
                    schemaResolver.AddSchema(type, false, jschema);
                }
                JsonReferenceResolver referenceResolver =
                    new JsonReferenceResolver(schemaResolver);
                foreach (var reference in refers)
                {
                    JsonSchema jschema =
                        dict_schema_name_to_JsonSchema[reference.Name];
                    referenceResolver.AddDocumentReference(reference.Name, jschema);
                }
                return referenceResolver;
            };

            string root_str = root.SchemaString;
            JObject schema = JObject.Parse(root_str);
            string schemaId = (string)schema["$id"];
            JsonSchema root_schema = JsonSchema.FromJsonAsync(root_str, schemaId, factory).Result;
            return root_schema;
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
        public JsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerConfig config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;

            this.schema = this.jsonSchemaGeneratorSettings == null
                ? NJsonSchema.JsonSchema.FromType<T>()
                : NJsonSchema.JsonSchema.FromType<T>(this.jsonSchemaGeneratorSettings);
            this.schemaFullname = schema.Title;
            this.schemaText = schema.ToJson();

            if (config == null) { return; }

            var nonJsonConfig = config.Where(item => !item.Key.StartsWith("json."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonSerializer: unknown configuration parameter {nonJsonConfig.First().Key}");
            }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.LatestCompatibilityStrict != null) { this.latestCompatibilityStrict = config.LatestCompatibilityStrict.Value; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"JsonSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        /// <summary>
        ///     Initialize a new instance of the JsonSerializer class.
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
        public JsonSerializer(ISchemaRegistryClient schemaRegistryClient, Schema schema, JsonSerializerConfig config = null, JsonSchemaGeneratorSettings jsonSchemaGeneratorSettings = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.jsonSchemaGeneratorSettings = jsonSchemaGeneratorSettings;
            foreach (var reference in schema.References)
            {
                ReferenceList.Add(reference);
            }

            create_schema_dict_util(schema);
            JsonSchema jsonSchema = getSchemaUtil(schema);
            this.schema = jsonSchema;

            this.schemaText = schema.SchemaString;
            this.schemaFullname = jsonSchema.Title;

            if (config == null) { return; }

            var nonJsonConfig = config.Where(item => !item.Key.StartsWith("json."));
            if (nonJsonConfig.Count() > 0)
            {
                throw new ArgumentException($"JsonSerializer: unknown configuration parameter {nonJsonConfig.First().Key}");
            }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.LatestCompatibilityStrict != null) { this.latestCompatibilityStrict = config.LatestCompatibilityStrict.Value; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"JsonSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a UTF8 encoded JSON 
        ///     represenation. The serialized data is preceeded by:
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
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            if (value == null) { return null; }

            var serializedString = Newtonsoft.Json.JsonConvert.SerializeObject(value, this.jsonSchemaGeneratorSettings?.ActualSerializerSettings);
            var validationResult = validator.Validate(serializedString, this.schema);
            if (validationResult.Count > 0)
            {
                throw new InvalidDataException("Schema validation failed for properties: [" + string.Join(", ", validationResult.Select(r => r.Path)) + "]");
            }

            try
            {
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    string subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(context, this.schemaFullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : context.Component == MessageComponentType.Key
                            ? schemaRegistryClient.ConstructKeySubjectName(context.Topic, this.schemaFullname)
                            : schemaRegistryClient.ConstructValueSubjectName(context.Topic, this.schemaFullname);

                    if (!subjectsRegistered.Contains(subject))
                    {
                        if (autoRegisterSchema)
                        {
                            schemaId = await schemaRegistryClient.RegisterSchemaAsync(subject,
                                        new Schema(this.schemaText, ReferenceList, SchemaType.Json), normalizeSchemas)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        else if (useLatestVersion)
                        {
                            var latestSchema = await schemaRegistryClient.GetLatestSchemaAsync(subject)
                                .ConfigureAwait(continueOnCapturedContext: false);
                            if (latestCompatibilityStrict)
                            {
                                var isCompatible = await schemaRegistryClient.IsCompatibleAsync(subject, new Schema(this.schemaText, ReferenceList, SchemaType.Json))
                                    .ConfigureAwait(continueOnCapturedContext: false);
                                if (!isCompatible)
                                {
                                    throw new InvalidDataException("Schema not compatible with latest schema : " + latestSchema.SchemaString);
                                }
                            }
                            schemaId = latestSchema.Id;
                        }
                        else
                        {
                            schemaId = await schemaRegistryClient.GetSchemaIdAsync(subject,
                                        new Schema(this.schemaText, ReferenceList, SchemaType.Json), normalizeSchemas)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serializeMutex.Release();
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
    }
}
