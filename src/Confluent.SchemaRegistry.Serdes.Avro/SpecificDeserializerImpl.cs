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
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class SpecificDeserializerImpl<T> : IAvroDeserializerImpl<T>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> datumReaderBySchemaId 
            = new Dictionary<int, DatumReader<T>>();

        private SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        /// <summary>
        ///     The Avro schema used to read values of type <typeparamref name="T"/>
        /// </summary>
        public global::Avro.Schema ReaderSchema { get; private set; }

        private ISchemaRegistryClient schemaRegistryClient;

        public SpecificDeserializerImpl(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;

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
        }

        public async Task<T> Deserialize(string topic, byte[] array)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                if (array.Length < 5)
                {
                    throw new InvalidDataException($"Expecting data framing of length 5 bytes or more but total data size is {array.Length} bytes");
                }

                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {array[0]}, expecting {Constants.MagicByte}");
                    }
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    DatumReader<T> datumReader;
                    await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                    try
                    {
                        datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                        if (datumReader == null)
                        {
                            if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                            {
                                datumReaderBySchemaId.Clear();
                            }

                            var writerSchemaJson = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                            var writerSchema = global::Avro.Schema.Parse(writerSchemaJson.SchemaString);

                            datumReader = new SpecificReader<T>(writerSchema, ReaderSchema);
                            datumReaderBySchemaId[writerId] = datumReader;
                        }
                    }
                    finally
                    {
                        deserializeMutex.Release();
                    }

                    if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
                    {
                        // This is a generic deserializer and it knows the type that needs to be serialized into. 
                        // Passing default(T) will result in null value and that will force the datumRead to
                        // use the schema namespace and name provided in the schema, which may not match (T).
                        var reuse = Activator.CreateInstance<T>();
                        return datumReader.Read(reuse, new BinaryDecoder(stream));
                    }

                    return datumReader.Read(default(T), new BinaryDecoder(stream));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

    }
}
