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
using Confluent.Kafka.SchemaRegistry;
using System.Reflection;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro specific serializer. Used to serialize primitive types
    ///     or types generated with avrogen tool.
    /// </summary>
    public class ConfluentAvroSerializer<T> : ISerializer<T>
    {
        // [0] : magic byte (use to identify protocol format)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        /// <summary>
        ///     Magic byte identifying avro confluent protocol format.
        /// </summary>
        public const byte MAGIC_BYTE = 0;
        
        private SpecificDatumWriter<T> avroWriter;

        /// <summary>
        ///     SchemaId corresponding to type <see cref="T"/>
        ///     Available only after Produce has been called once
        /// </summary>
        public int SchemaId { get; private set; }
        private int schemaIdBigEndian; // schema id in big endian (bytes reversed)

        // topics alread
        private HashSet<string> topicsRegistred = new HashSet<string>();

		/// <summary>
        ///		Client used to communicate with confluent schema registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegisterClient { get; }

		/// <summary>
        ///		Indicates if this serializer is used for kafka keys or values.
        /// </summary>
        public bool IsKey { get; }

        /// <summary>
        ///     The avro schema corresponding to type <see cref="T"/>
        /// </summary>
        public Avro.Schema WriterSchema { get; }

        /// <summary>
        ///     Initial capcity used for memory stream.
        /// </summary>
        /// <remarks>
        ///     Use a value high enough to avoid resizing of memorystream
        ///     and small enough to avoid too big allocation.
        ///     You will have to monitor the size of <see cref="Serialize(string, T)"/>
        ///     to take correct value.
        /// </remarks>
        public int MemoryStreamInitialCapavity { get; set; } = 30;


        /// <summary>
        ///     Initiliaze an avro serializer.
        /// </summary>
        /// <param name="schemaRegisterClient">
        ///		Client used to communicate with confluent schema registry.
        ///	</param>
        /// <param name="isKey">
        ///		Indicates if this serializer is used for kafka keys or values.
        ///	</param>
        /// <exception cref="InvalidOperationException">
        ///		The generic type <see cref="T"/> is not supported.
        ///	</exception>
        public ConfluentAvroSerializer(ISchemaRegistryClient schemaRegisterClient, bool isKey)
        {
            SchemaRegisterClient = schemaRegisterClient;
            IsKey = isKey;

            Type writerType = typeof(T);
            if (typeof(ISpecificRecord).IsAssignableFrom(writerType) || writerType.IsSubclassOf(typeof(SpecificFixed)))
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
                throw new InvalidOperationException($"{nameof(ConfluentAvroSerializer<T>)} " +
                    "only accepts int, bool, double, string, float, long, byte[], " +
                    "ISpecificRecord subclass and SpecificFixed");
            }
            avroWriter = new SpecificDatumWriter<T>(WriterSchema);
        }

        /// <summary>
        ///     Serialize an instance of type T to a byte array
        ///     corresponding to confluent avro format with schema registry.
        ///     May block or throw at first call when registring schema at schema registry.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated wih the data.
        /// </param>
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        public byte[] Serialize(string topic, T data)
        {
            if (!topicsRegistred.Contains(topic))
            {
                // first usage: register schema, to check compatibility and version
                string subject = SchemaRegisterClient.GetRegistrySubject(topic, IsKey);
                // schemaId could already be intialized through an other topic
                // this has no impact, as schemaId will be the same
                SchemaId = SchemaRegisterClient.RegisterAsync(subject, WriterSchema.ToString()).Result;
                // use big endian
                schemaIdBigEndian = IPAddress.NetworkToHostOrder(SchemaId);
                topicsRegistred.Add(topic);
            }
			
            using (var stream = new MemoryStream(MemoryStreamInitialCapavity))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(MAGIC_BYTE);

                writer.Write(schemaIdBigEndian);
                avroWriter.Write(data, new BinaryEncoder(stream));

                // TODO
                // stream.ToArray create a copy of the array
                // we could return GetBuffer (or GetArraySegment in netstandard) 
                // with proper length / offset to avoid this copy
                // which would require to change ISerializer interface
                return stream.ToArray();
            }
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            // TODO not necessary for first iteration
            return config;
        }
    }
}