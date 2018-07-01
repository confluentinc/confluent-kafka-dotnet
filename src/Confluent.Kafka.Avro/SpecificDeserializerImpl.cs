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
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.SchemaRegistry;


namespace Confluent.Kafka.Serialization
{
    internal class SpecificDeserializerImpl<T> : IAvroDeserializerImpl<T>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> datumReaderBySchemaId 
            = new Dictionary<int, DatumReader<T>>();

        private object deserializeLockObj = new object();

        /// <summary>
        ///     The avro schema used to read values of type <typeparamref name="T"/>
        /// </summary>
        public Avro.Schema ReaderSchema { get; private set; }

        private ISchemaRegistryClient schemaRegistryClient;

        public SpecificDeserializerImpl(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;

            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)))
            {
                ReaderSchema = (Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (typeof(T).Equals(typeof(int)))
            {
                ReaderSchema = Avro.Schema.Parse("int");
            }
            else if (typeof(T).Equals(typeof(bool)))
            {
                ReaderSchema = Avro.Schema.Parse("boolean");
            }
            else if (typeof(T).Equals(typeof(double)))
            {
                ReaderSchema = Avro.Schema.Parse("double");
            }
            else if (typeof(T).Equals(typeof(string)))
            {
                ReaderSchema = Avro.Schema.Parse("string");
            }
            else if (typeof(T).Equals(typeof(float)))
            {
                ReaderSchema = Avro.Schema.Parse("float");
            }
            else if (typeof(T).Equals(typeof(long)))
            {
                ReaderSchema = Avro.Schema.Parse("long");
            }
            else if (typeof(T).Equals(typeof(byte[])))
            {
                ReaderSchema = Avro.Schema.Parse("bytes");
            }
            else if (typeof(T).Equals(typeof(Null)))
            {
                ReaderSchema = Avro.Schema.Parse("null");
            }
            else
            {
                throw new ArgumentException(
                    $"{nameof(AvroDeserializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }
        }

        public T Deserialize(string topic, byte[] array)
        {
            // Note: topic is not necessary for deserialization (or knowing if it's a key 
            // or value) only the schema id is needed.

            using (var stream = new MemoryStream(array))
            using (var reader = new BinaryReader(stream))
            {
                var magicByte = reader.ReadByte();
                if (magicByte != Constants.MagicByte)
                {
                    // may change in the future.
                    throw new InvalidDataException($"magic byte should be 0, not {magicByte}");
                }
                var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                DatumReader<T> datumReader;
                lock (deserializeLockObj)
                {
                    datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                    if (datumReader == null)
                    {
                        if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                        {
                            datumReaderBySchemaId.Clear();
                        }

                        var writerSchemaJson = schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false).GetAwaiter().GetResult();
                        var writerSchema = Avro.Schema.Parse(writerSchemaJson);

                        datumReader = new SpecificReader<T>(writerSchema, ReaderSchema);
                        datumReaderBySchemaId[writerId] = datumReader;
                    }
                }
                return datumReader.Read(default(T), new BinaryDecoder(stream));
            }
        }

    }
}
