// Copyright 2018 Confluent Inc.
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
using System.Threading.Tasks;
using Avro;
using Confluent.Kafka;
using Avro.Generic;
using Avro.IO;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class GenericSerializerImpl : AsyncSerializer<GenericRecord, Avro.Schema>
    {
        private Dictionary<Avro.Schema, string> knownSchemas =
            new Dictionary<global::Avro.Schema, string>();
        private Dictionary<KeyValuePair<string, string>, SchemaId> registeredSchemas =
            new Dictionary<KeyValuePair<string, string>, SchemaId>();

        public GenericSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            AvroSerializerConfig config,
            RuleRegistry ruleRegistry) : base(schemaRegistryClient, config, ruleRegistry)
        {
            if (config == null) { return; }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseSchemaId != null) { this.useSchemaId = config.UseSchemaId.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            this.subjectNameStrategy = (config.SubjectNameStrategy ?? SubjectNameStrategy.Associated).ToAsyncDelegate(schemaRegistryClient, config);
            if (config.SchemaIdStrategy != null) { this.schemaIdEncoder = config.SchemaIdStrategy.Value.ToEncoder(); }

            if (this.useLatestVersion && this.autoRegisterSchema)
            {
                throw new ArgumentException($"AvroSerializer: cannot enable both use.latest.version and auto.register.schemas");
            }
        }

        public override async Task<byte[]> SerializeAsync(GenericRecord value, SerializationContext context)
        {
            return await Serialize(context.Topic, context.Headers, value,
                    context.Component == MessageComponentType.Key)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        /// <summary>
        ///     Serialize GenericRecord instance to a byte array in Avro format. The serialized
        ///     data is preceded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order). This call may block or throw 
        ///     on first use for a particular topic during schema registration.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data.
        /// </param>
        /// <param name="headers">
        ///     The headers associated with the data.
        /// </param>
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <param name="isKey">
        ///     whether or not the data represents a message key.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        public async Task<byte[]> Serialize(string topic, Headers headers, GenericRecord data, bool isKey)
        {
            try
            {
                SchemaId schemaId;
                string subject;
                RegisteredSchema latestSchema;
                Avro.Schema writerSchema;
                await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    // TODO: If any of these caches fills up, this is probably an
                    // indication of misuse of the serializer. Ideally we would do 
                    // something more sophisticated than the below + not allow 
                    // the misuse to keep happening without warning.
                    if (knownSchemas.Count > schemaRegistryClient.MaxCachedSchemas ||
                        registeredSchemas.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        knownSchemas.Clear();
                        registeredSchemas.Clear();
                    }

                    // Determine a schema string corresponding to the schema object.
                    // TODO: It would be more efficient to use a hash function based
                    // on the instance reference, not the implementation provided by 
                    // Schema.
                    writerSchema = data.Schema;
                    string writerSchemaString;
                    if (knownSchemas.ContainsKey(writerSchema))
                    {
                        writerSchemaString = knownSchemas[writerSchema];
                    }
                    else
                    {
                        writerSchemaString = writerSchema.ToString();
                        knownSchemas.Add(writerSchema, writerSchemaString);
                    }

                    // Verify schema compatibility (& register as required) + get the 
                    // id corresponding to the schema.
                    
                    // TODO: Again, the hash functions in use below are potentially 
                    // slow since writerSchemaString is potentially long. It would be
                    // better to use hash functions based on the writerSchemaString 
                    // object reference, not value.

                    subject = await GetSubjectName(topic, isKey, data.Schema.Fullname).ConfigureAwait(false);
                    var subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchemaString);
                    latestSchema = await GetReaderSchema(subject)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    if (latestSchema != null)
                    {
                        schemaId = new SchemaId(SchemaType.Avro, latestSchema.Id, latestSchema.Guid);
                    }
                    else if (!registeredSchemas.TryGetValue(subjectSchemaPair, out schemaId))
                    {
                        // first usage: register/get schema to check compatibility
                        var inputSchema = new Schema(writerSchemaString, new List<SchemaReference>(), SchemaType.Avro);
                        var outputSchema = autoRegisterSchema
                            ? await schemaRegistryClient
                                .RegisterSchemaWithResponseAsync(subject, inputSchema, normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false)
                            : await schemaRegistryClient
                                .LookupSchemaAsync(subject, inputSchema, ignoreDeletedSchemas: true, normalize: normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false);

                        schemaId = new SchemaId(SchemaType.Avro, outputSchema.Id, outputSchema.Guid);
                        registeredSchemas.Add(subjectSchemaPair, schemaId);
                    }
                }
                finally
                {
                    serdeMutex.Release();
                }

                if (latestSchema != null)
                {
                    writerSchema = await GetParsedSchema(latestSchema).ConfigureAwait(false);
                    FieldTransformer fieldTransformer = async (ctx, transform, message) => 
                    {
                        return await AvroUtils.Transform(ctx, writerSchema, message, transform).ConfigureAwait(false);
                    };
                    data = await ExecuteRules(isKey, subject, topic, headers, RuleMode.Write,
                            null, latestSchema, data, fieldTransformer)
                        .ContinueWith(t => (GenericRecord)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
                
                SerializationContext context = new SerializationContext(
                    isKey ? MessageComponentType.Key : MessageComponentType.Value, topic, headers);

                using (var stream = new MemoryStream(initialBufferSize))
                {
                    new GenericWriter<GenericRecord>(writerSchema)
                        .Write(data, new BinaryEncoder(stream));
                    
                    var buffer = await ExecuteRules(isKey, subject, topic, headers, RulePhase.Encoding, RuleMode.Write,
                            null, latestSchema, stream.ToArray(), null)
                        .ContinueWith(t => (byte[])t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    var schemaIdSize = schemaIdEncoder.CalculateSize(ref schemaId);
                    var serializedMessageSize = (int) buffer.Length;

                    var result = new byte[schemaIdSize + serializedMessageSize];
                    schemaIdEncoder.Encode(result, ref context, ref schemaId);
                    buffer.AsSpan(0, serializedMessageSize).CopyTo(result.AsSpan(schemaIdSize));
                    
                    return result;
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
        
        protected override async Task<Avro.Schema> ParseSchema(Schema schema)
        {
            SchemaNames namedSchemas = await AvroUtils.ResolveNamedSchema(schema, schemaRegistryClient)
                .ConfigureAwait(continueOnCapturedContext: false);
            return Avro.Schema.Parse(schema.SchemaString, namedSchemas);
        }
    }
}
