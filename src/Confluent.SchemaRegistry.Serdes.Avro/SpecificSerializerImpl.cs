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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class SpecificSerializerImpl<T> : IAvroSerializerImpl<T> where T : class
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private int initialBufferSize;
        private SubjectNameStrategyDelegate subjectNameStrategy;

        private string writerSchemaString;
        private global::Avro.Schema writerSchema;

        /// <remarks>
        ///     A given schema is uniquely identified by a schema id, even when
        ///     registered against multiple subjects.
        /// </remarks>
        private int? writerSchemaId;

        private SpecificWriter<T> avroWriter;

        private HashSet<string> subjectsRegistered = new HashSet<string>();

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        public SpecificSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            bool autoRegisterSchema,
            int initialBufferSize,
            SubjectNameStrategyDelegate subjectNameStrategy)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.autoRegisterSchema = autoRegisterSchema;
            this.initialBufferSize = initialBufferSize;
            this.subjectNameStrategy = subjectNameStrategy;

            Type writerType = typeof(T);
            if (typeof(ISpecificRecord).IsAssignableFrom(writerType))
            {
                writerSchema = (global::Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (writerType.Equals(typeof(int)))
            {
                writerSchema = global::Avro.Schema.Parse("int");
            }
            else if (writerType.Equals(typeof(bool)))
            {
                writerSchema = global::Avro.Schema.Parse("boolean");
            }
            else if (writerType.Equals(typeof(double)))
            {
                writerSchema = global::Avro.Schema.Parse("double");
            }
            else if (writerType.Equals(typeof(string)))
            {
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET string type, however we don't for consistency
                // with the Java Avro serializer.
                writerSchema = global::Avro.Schema.Parse("string");
            }
            else if (writerType.Equals(typeof(float)))
            {
                writerSchema = global::Avro.Schema.Parse("float");
            }
            else if (writerType.Equals(typeof(long)))
            {
                writerSchema = global::Avro.Schema.Parse("long");
            }
            else if (writerType.Equals(typeof(byte[])))
            {
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET byte[] type, however we don't for consistency
                // with the Java Avro serializer.
                writerSchema = global::Avro.Schema.Parse("bytes");
            }
            else if (writerType.Equals(typeof(Null)))
            {
                writerSchema = global::Avro.Schema.Parse("null");
            }
            else
            {
                throw new InvalidOperationException(
                    $"AvroSerializer only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }

            avroWriter = new SpecificWriter<T>(writerSchema);
            writerSchemaString = writerSchema.ToString();
        }

        public async Task<byte[]> Serialize(string topic, T data, bool isKey)
        {
            try
            {
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    string fullname = null;
                    if (data is ISpecificRecord && ((ISpecificRecord)data).Schema is Avro.RecordSchema)
                    {
                        fullname = ((Avro.RecordSchema)((ISpecificRecord)data).Schema).Fullname;
                    }

                    string subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic), fullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : isKey
                            ? schemaRegistryClient.ConstructKeySubjectName(topic, fullname)
                            : schemaRegistryClient.ConstructValueSubjectName(topic, fullname);

                    if (!subjectsRegistered.Contains(subject))
                    {
                        // first usage: register/get schema to check compatibility
                        writerSchemaId = autoRegisterSchema
                            ? await schemaRegistryClient.RegisterSchemaAsync(subject, writerSchemaString).ConfigureAwait(continueOnCapturedContext: false)
                            : await schemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString).ConfigureAwait(continueOnCapturedContext: false);

                        subjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serializeMutex.Release();
                }

                if (data == null)
                {
                    return null;
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);

                    writer.Write(IPAddress.HostToNetworkOrder(writerSchemaId.Value));
                    avroWriter.Write(data, new BinaryEncoder(stream));

                    // TODO: maybe change the ISerializer interface so that this copy isn't necessary.
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
