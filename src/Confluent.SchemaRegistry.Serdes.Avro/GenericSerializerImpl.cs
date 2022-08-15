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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Avro.Generic;
using Avro.IO;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class GenericSerializerImpl : IAvroSerializerImpl<GenericRecord>
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private bool normalizeSchemas;
        private bool useLatestVersion;
        private int initialBufferSize;
        private SubjectNameStrategyDelegate subjectNameStrategy;

        private Dictionary<global::Avro.RecordSchema, string> knownSchemas = new Dictionary<global::Avro.RecordSchema, string>();
        private HashSet<KeyValuePair<string, string>> registeredSchemas = new HashSet<KeyValuePair<string, string>>();
        private Dictionary<string, int> schemaIds = new Dictionary<string, int>();

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        public GenericSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            bool autoRegisterSchema,
            bool normalizeSchemas,
            bool useLatestVersion,
            int initialBufferSize,
            SubjectNameStrategyDelegate subjectNameStrategy)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.autoRegisterSchema = autoRegisterSchema;
            this.normalizeSchemas = normalizeSchemas;
            this.useLatestVersion = useLatestVersion;
            this.initialBufferSize = initialBufferSize;
            this.subjectNameStrategy = subjectNameStrategy;
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
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <param name="isKey">
        ///     whether or not the data represents a message key.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        public async Task<byte[]> Serialize(string topic, GenericRecord data, bool isKey)
        {
            try
            {
                int schemaId;
                global::Avro.RecordSchema writerSchema;
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
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
                    string writerSchemaString = null;
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

                    string subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic), data.Schema.Fullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : isKey
                            ? schemaRegistryClient.ConstructKeySubjectName(topic, data.Schema.Fullname)
                            : schemaRegistryClient.ConstructValueSubjectName(topic, data.Schema.Fullname);

                    var subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchemaString);
                    if (!registeredSchemas.Contains(subjectSchemaPair))
                    {
                        int newSchemaId;
                        if (useLatestVersion)
                        {
                            var latestSchema = await schemaRegistryClient.GetLatestSchemaAsync(subject)
                                .ConfigureAwait(continueOnCapturedContext: false);
                            newSchemaId = latestSchema.Id;
                        }
                        else
                        {
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
                    }
                    schemaId = schemaIds[writerSchemaString];
                }
                finally
                {
                    serializeMutex.Release();
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
    }
}
