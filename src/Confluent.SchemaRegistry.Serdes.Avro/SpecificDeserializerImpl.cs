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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Avro;
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class SpecificDeserializerImpl<T> : AsyncDeserializer<T, Avro.Schema>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<(Avro.Schema, Avro.Schema), DatumReader<T>> datumReaderBySchema 
            = new Dictionary<(Avro.Schema, Avro.Schema), DatumReader<T>>();

        /// <summary>
        ///     The Avro schema used to read values of type <typeparamref name="T"/>
        /// </summary>
        public global::Avro.Schema ReaderSchema { get; private set; }

        public SpecificDeserializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            AvroDeserializerConfig config,
            RuleRegistry ruleRegistry) : base(schemaRegistryClient, config, ruleRegistry)
        {
            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                ReaderSchema = ((ISpecificRecord)Activator.CreateInstance<T>()).Schema;
            }
            else if (typeof(T).Equals(typeof(int)))
            {
                ReaderSchema = global::Avro.Schema.Parse("int");
            }
            else if (typeof(T).Equals(typeof(bool)))
            {
                ReaderSchema = global::Avro.Schema.Parse("boolean");
            }
            else if (typeof(T).Equals(typeof(double)))
            {
                ReaderSchema = global::Avro.Schema.Parse("double");
            }
            else if (typeof(T).Equals(typeof(string)))
            {
                ReaderSchema = global::Avro.Schema.Parse("string");
            }
            else if (typeof(T).Equals(typeof(float)))
            {
                ReaderSchema = global::Avro.Schema.Parse("float");
            }
            else if (typeof(T).Equals(typeof(long)))
            {
                ReaderSchema = global::Avro.Schema.Parse("long");
            }
            else if (typeof(T).Equals(typeof(byte[])))
            {
                ReaderSchema = global::Avro.Schema.Parse("bytes");
            }
            else if (typeof(T).Equals(typeof(Null)))
            {
                ReaderSchema = global::Avro.Schema.Parse("null");
            }
            else
            {
                throw new InvalidOperationException(
                    $"{nameof(AvroDeserializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }
            
            if (config == null) { return; }

            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(schemaRegistryClient, config); }
            if (config.SchemaIdStrategy != null) { this.schemaIdDecoder = config.SchemaIdStrategy.Value.ToDeserializer(); }
        }

        public override async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull,
            SerializationContext context)
        {
            return isNull
                ? default
                : await Deserialize(context.Topic, context.Headers, data,
                    context.Component == MessageComponentType.Key).ConfigureAwait(false);
        }
        
        public async Task<T> Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> array, bool isKey)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                string subject = GetSubjectName(topic, isKey, null);
                RegisteredSchema latestSchema = null;
                if (subject != null)
                {
                    latestSchema = await GetReaderSchema(subject)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                Schema writerSchemaJson;
                Avro.Schema writerSchema;
                object data;
                IList<Migration> migrations = new List<Migration>();
                SerializationContext context = new SerializationContext(
                    isKey ? MessageComponentType.Key : MessageComponentType.Value, topic, headers);
                SchemaId writerId = new SchemaId(SchemaType.Avro);
                var payload = schemaIdDecoder.Decode(array, context, ref writerId);
                
                (writerSchemaJson, writerSchema) = await GetWriterSchema(subject, writerId).ConfigureAwait(false);
                if (subject == null)
                {
                    subject = GetSubjectName(topic, isKey, writerSchema.Fullname);
                    if (subject != null)
                    {
                        latestSchema = await GetReaderSchema(subject)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }
                }
                payload = await ExecuteRules(isKey, subject, topic, headers, RulePhase.Encoding, RuleMode.Read,
                        null, writerSchemaJson, payload, null)
                    .ContinueWith(t => t.Result is byte[] bytes ? new ReadOnlyMemory<byte>(bytes) : (ReadOnlyMemory<byte>)t.Result)
                    .ConfigureAwait(continueOnCapturedContext: false);

                if (latestSchema != null)
                {
                    migrations = await GetMigrations(subject, writerSchemaJson, latestSchema)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                DatumReader<T> datumReader = null;
                if (migrations.Count > 0)
                {
                    // If the writer schema is a simple bytes type, read bytes directly
                    if (writerSchema.Tag == Avro.Schema.Type.Bytes)
                    {
                        data = payload.ToArray();
                    }
                    else
                    {
                        using (var memoryStream = new ReadOnlyMemoryStream(payload))
                        {
                            data = new GenericReader<GenericRecord>(writerSchema, writerSchema)
                                .Read(default(GenericRecord), new BinaryDecoder(memoryStream));
                        }
                    }
                        
                    string jsonString = null;
                    using (var jsonStream = new MemoryStream())
                    {
                        GenericRecord record = (GenericRecord)data;
                        DatumWriter<object> datumWriter = new GenericDatumWriter<object>(writerSchema);

                        JsonEncoder encoder = new JsonEncoder(writerSchema, jsonStream);
                        datumWriter.Write(record, encoder);
                        encoder.Flush();

                        jsonString = Encoding.UTF8.GetString(jsonStream.ToArray());
                    }
                        
                    JToken json = JToken.Parse(jsonString);
                    json = await ExecuteMigrations(migrations, isKey, subject, topic, headers, json)
                        .ContinueWith(t => (JToken)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    Avro.IO.Decoder decoder = new JsonDecoder(ReaderSchema, json.ToString(Formatting.None));
                        
                    datumReader = new SpecificReader<T>(ReaderSchema, ReaderSchema);
                    data = Read(datumReader, decoder);
                }
                else
                {
                    // If the writer schema is a simple bytes type, read bytes directly
                    if (writerSchema.Tag == Avro.Schema.Type.Bytes)
                    {
                        data = payload.ToArray();
                    }
                    else
                    {
                        datumReader = await GetDatumReader(writerSchema, ReaderSchema).ConfigureAwait(false);

                        using var stream = new ReadOnlyMemoryStream(payload);
                        data = Read(datumReader, new BinaryDecoder(stream));
                    }
                }

                Schema readerSchemaJson = latestSchema ?? writerSchemaJson;
                Avro.Schema readerSchema = latestSchema != null
                    ? await GetParsedSchema(latestSchema).ConfigureAwait(false)
                    : writerSchema;
                FieldTransformer fieldTransformer = async (ctx, transform, message) =>
                {
                    return await AvroUtils.Transform(ctx, readerSchema, message, transform).ConfigureAwait(false);
                };
                data = await ExecuteRules(isKey, subject, topic, headers, RuleMode.Read,
                        null, readerSchemaJson, data, fieldTransformer)
                    .ConfigureAwait(continueOnCapturedContext: false);

                return (T) data;
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

        private async Task<DatumReader<T>> GetDatumReader(Avro.Schema writerSchema, Avro.Schema readerSchema)
        {
            DatumReader<T> datumReader = null;
            await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (datumReaderBySchema.TryGetValue((writerSchema, readerSchema), out datumReader))
                {
                    return datumReader;

                }
                else
                {
                    if (datumReaderBySchema.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        datumReaderBySchema.Clear();
                    }

                    if (readerSchema == null)
                    {
                        readerSchema = writerSchema;
                    }
                    datumReader = new SpecificReader<T>(writerSchema, readerSchema);
                    datumReaderBySchema[(writerSchema, readerSchema)] = datumReader;
                    return datumReader;
                }
            }
            finally
            {
                serdeMutex.Release();
            }
        }

        private static object Read(DatumReader<T> datumReader, Avro.IO.Decoder decoder)
        {
            object data;
            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                // This is a generic deserializer and it knows the type that needs to be serialized into. 
                // Passing default(T) will result in null value and that will force the datumRead to
                // use the schema namespace and name provided in the schema, which may not match (T).
                var reuse = Activator.CreateInstance<T>();
                data = datumReader.Read(reuse, decoder);
            }
            else
            {
                data = datumReader.Read(default(T), decoder);
            }

            return data;
        }
    }
}
