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
        // We use the same format as confluentinc java implementation for compatibility :

        // [0] : magic byte (use to identify protocol format)
        // [1-4] : unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        // following: data serialized with corresponding schema

        // topic refer to kafka topic
        // subject refers to schema registry subject. Usually topic postfixed by -key or -data

        private const byte MAGIC_BYTE = 0;
        private const int UNINITIALIZED_ID = -1;
        private const int INIT_STREAM_CAPACITY = 30;

        private SpecificDatumWriter<T> avroWriter;
        private int schemaId = UNINITIALIZED_ID;
        private HashSet<string> topicsRegistred = new HashSet<string>();

		/// <summary>
        ///		Client used to communicate with confluent schema registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegisterClient { get; }

		/// <summary>
        ///		Indicates if this serializer is used for kafka keys or values.
        /// </summary>
        public bool IsKey { get; }

        public Avro.Schema WriterSchema { get; }

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
        ///		The <see cref="T"/> generic type is not supported.
        ///	</exception>
        public ConfluentAvroSerializer(ISchemaRegistryClient schemaRegisterClient, bool isKey)
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
                throw new InvalidOperationException($"{nameof(ConfluentAvroSerializer)} " +
                    $"only accepts int, bool, double, string, float, long, byte[], " +
                    "ISpecificRecord subclass and SpecificRecord");
            }
            avroWriter = new SpecificDatumWriter<T>(WriterSchema);
        }


        public byte[] Serialize(string topic, T data)
        {
            if (topicsRegistred.Contains(topic))
            {
                // first usage: register schema, to check compatibility and version
                var subject = SchemaRegisterClient.GetRegistrySubject(topic, IsKey);
				// TODO SerializeAsync would be fine here. Serialize could block, which is bad.
                schemaId = SchemaRegisterClient.RegisterAsync(subject, WriterSchema.ToString()).Result;
                // use big endian
                schemaId = IPAddress.NetworkToHostOrder(schemaId);
                topicsRegistred.Add(topic);
            }
			
            using (var stream = new MemoryStream(INIT_STREAM_CAPACITY))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(MAGIC_BYTE);

                writer.Write(schemaId);
                avroWriter.Write(data, new BinaryEncoder(stream));

				// TODO
				// stream.ToArray create a copy of the array
				// we could return GetBuffer (or GetArraySegment in netstandard) with proper length
				// to avoid this copy, which would need to change ISerializer interface
                return stream.ToArray();
            }
        }

        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}