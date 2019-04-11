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
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class SpecificSerializerImpl<T> : IAvroSerializerImpl<T>
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private int initialBufferSize;

        private string writerSchemaString;
        private global::Avro.Schema writerSchema;
        private int? writerSchemaId;

        private SpecificWriter<T> avroWriter;
       
        private HashSet<string> subjectsRegistered = new HashSet<string>();

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        public SpecificSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            bool autoRegisterSchema,
            int initialBufferSize)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.autoRegisterSchema = autoRegisterSchema;
            this.initialBufferSize = initialBufferSize;

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
                await serializeMutex.WaitAsync();
                try
                {
                    string subject = isKey
                        ? schemaRegistryClient.ConstructKeySubjectName(topic)
                        : schemaRegistryClient.ConstructValueSubjectName(topic);

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
