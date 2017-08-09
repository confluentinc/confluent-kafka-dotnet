// Copyright 2016-2017 Confluent Inc.
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
using Avro.Specific;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka.SchemaRegistry;
using System.Reflection;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro generic deserializer
    /// </summary>
    public class AvroSerializer<T>
    {
        //We use the same format as confluentinc java implementation for compatibility :

        // [0] : Magic byte (0 as of today, used for future version with breaking change)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        public const byte MAGIC_BYTE = 0;

        private ISchemaRegistryClient SchemaRegisterClient { get; }

        // maintain a cache of deserializer, so that we only have to construct it once
        private readonly Dictionary<Avro.Schema, DatumWriter<object>> writerBySchemaId = new Dictionary<Avro.Schema, DatumWriter<object>>();

        public bool IsKey { get; }

        public Avro.Schema WriterSchema { get; }

        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegisterClient"></param>
        public AvroSerializer(ISchemaRegistryClient schemaRegisterClient, bool isKey)
        {
            SchemaRegisterClient = schemaRegisterClient;
            IsKey = isKey;

            Type writerType = typeof(T);
            if (writerType.IsSubclassOf(typeof(ISpecificRecord)) || writerType.IsSubclassOf(typeof(SpecificFixed)))
            {
                WriterSchema = (Avro.Schema)typeof(T).GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (writerType.Equals(typeof(int)))
            {
                WriterSchema = Avro.Schema.Parse("int");
            }
            else if (writerType.Equals(typeof(bool)))
            {
                WriterSchema = Avro.Schema.Parse("boolean");
            }
            else if (writerType.Equals(typeof(double)))
            {
                WriterSchema = Avro.Schema.Parse("double");
            }
            else if (writerType.Equals(typeof(string)))
            {
                WriterSchema = Avro.Schema.Parse("string");
            }
            else if (writerType.Equals(typeof(float)))
            {
                WriterSchema = Avro.Schema.Parse("float");
            }
            else if (writerType.Equals(typeof(long)))
            {
                WriterSchema = Avro.Schema.Parse("long");
            }
            else if (writerType.Equals(typeof(byte[])))
            {
                WriterSchema = Avro.Schema.Parse("bytes");
            }
            else
            {
                throw new NotImplementedException("TODO");
            }
        }

        /// <summary>
        ///     Generate a GenericReader.
        /// </summary>
        /// <param name="writerSchema">The writer schema</param>
        /// <returns></returns>
        protected virtual GenericReader<object> GenerateDeserializer(Avro.Schema writerSchema)
        {
            return new GenericReader<object>(writerSchema, writerSchema);
        }

        protected byte[] Serialize(string topic, T data, out Avro.Schema schema, out int writerId)
        {
            // topic not necessary for deserialization (knowing if it's key or not neither)
            // we only care about schema id

            using (var stream = new MemoryStream(30))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(MAGIC_BYTE);

                var subject = 
                SchemaRegisterClient.RegisterAsync()
                writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                if (!writerBySchemaId.TryGetValue(WriterSchema, out DatumWriter<object> datumWriter))
                {
                    // GetSchemaAsync may throw
                    string writerSchema = SchemaRegisterClient.GetSchemaAsync(writerId).Result;
                    schema = Avro.Schema.Parse(writerSchema);

                    // can be of multiple type: Record, Primitive...
                    // we don't read against a given schema, so writer and reader schema are same
                    genericReader = GenerateDeserializer(schema);
                    readerBySchemaId[writerId] = genericReader;
                }
                else
                {
                    schema = genericReader.ReaderSchema;
                }

                return genericReader.Read(genericReader, new BinaryDecoder(stream));
            }
        }
    }
