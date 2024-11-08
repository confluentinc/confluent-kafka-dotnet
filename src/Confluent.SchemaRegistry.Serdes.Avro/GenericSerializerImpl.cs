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
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Avro.Generic;
using Avro.IO;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class GenericSerializerImpl : AsyncSerializer<GenericRecord, Avro.Schema>
    {
        private Dictionary<Avro.Schema, string> knownSchemas = new Dictionary<global::Avro.Schema, string>();
        private HashSet<KeyValuePair<string, string>> registeredSchemas = new HashSet<KeyValuePair<string, string>>();
        private Dictionary<string, int> schemaIds = new Dictionary<string, int>();

        public GenericSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            AvroSerializerConfig config,
            RuleRegistry ruleRegistry) : base(schemaRegistryClient, config, ruleRegistry)
        {
            if (config == null) { return; }

            if (config.BufferBytes != null) { this.initialBufferSize = config.BufferBytes.Value; }
            if (config.AutoRegisterSchemas != null) { this.autoRegisterSchema = config.AutoRegisterSchemas.Value; }
            if (config.NormalizeSchemas != null) { this.normalizeSchemas = config.NormalizeSchemas.Value; }
            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }

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
                int schemaId;
                string subject;
                RegisteredSchema latestSchema = null;
                Avro.Schema writerSchema;
                await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    // TODO: If any of these caches fills up, this is probably an
                    // indication of misuse of the serializer. Ideally we would do 
                    // something more sophisticated than the below + not allow 
                    // the misuse to keep happening without warning.
                    if (knownSchemas.Count > schemaRegistryClient.MaxCachedSchemas ||
                        registeredSchemas.Count > schemaRegistryClient.MaxCachedSchemas ||
                        schemaIds.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        knownSchemas.Clear();
                        registeredSchemas.Clear();
                        schemaIds.Clear();
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

                    subject = GetSubjectName(topic, isKey, data.Schema.Fullname);
                    var subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchemaString);
                    latestSchema = await GetReaderSchema(subject)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    if (latestSchema != null)
                    {
                        schemaId = latestSchema.Id;
                    }
                    else if (!registeredSchemas.Contains(subjectSchemaPair))
                    {
                        int newSchemaId;

                        // first usage: register/get schema to check compatibility
                        if (autoRegisterSchema)
                        {
                            newSchemaId = await schemaRegistryClient
                                .RegisterSchemaAsync(subject, writerSchemaString, normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        else
                        {
                            newSchemaId = await schemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString, normalizeSchemas)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }

                        if (!schemaIds.ContainsKey(writerSchemaString))
                        {
                            schemaIds.Add(writerSchemaString, newSchemaId);
                        }
                        else if (schemaIds[writerSchemaString] != newSchemaId)
                        {
                            schemaIds.Clear();
                            registeredSchemas.Clear();
                            throw new KafkaException(new Error(isKey ? ErrorCode.Local_KeySerialization : ErrorCode.Local_ValueSerialization, $"Duplicate schema registration encountered: Schema ids {schemaIds[writerSchemaString]} and {newSchemaId} are associated with the same schema."));
                        }

                        registeredSchemas.Add(subjectSchemaPair);

                        schemaId = schemaIds[writerSchemaString];
                    }
                    else
                    {
                        schemaId = schemaIds[writerSchemaString];
                    }
                }
                finally
                {
                    serdeMutex.Release();
                }

                if (latestSchema != null)
                {
                    writerSchema = await GetParsedSchema(latestSchema);
                    FieldTransformer fieldTransformer = async (ctx, transform, message) => 
                    {
                        return await AvroUtils.Transform(ctx, writerSchema, message, transform).ConfigureAwait(false);
                    };
                    data = await ExecuteRules(isKey, subject, topic, headers, RuleMode.Write, null,
                        latestSchema, data, fieldTransformer)
                        .ContinueWith(t => (GenericRecord)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(schemaId));
                    new GenericWriter<GenericRecord>(writerSchema)
                        .Write(data, new BinaryEncoder(stream));
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
        
        protected override Task<Avro.Schema> ParseSchema(Schema schema)
        {
            return Task.FromResult(Avro.Schema.Parse(schema.SchemaString));
        }
    }
}
