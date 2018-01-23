// Copyright 2016-2018 Confluent Inc.
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
using System.Linq;
using System.IO;
using System.Net;
using Avro.Specific;
using Avro.IO;
using Confluent.SchemaRegistry;
using System.Reflection;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro specific serializer. Use this serializer with types generated
    ///     using the avrogen.exe tool or one of the following primitive types: 
    ///     int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroSerializer<T> : ISerializer<T>
    {
        private SpecificWriter<T> avroWriter;

        private HashSet<string> topicsRegistered = new HashSet<string>();

        private bool disposeClientOnDispose;

        private string writerSchemaString { get; set; }

        private const string InitialBufferSizePropertyName = "avro.serializer.buffer.bytes";

        private const string AutoRegisterSchemaPropertyName = "avro.serializer.auto.register.schemas";

        /// <summary>
        ///     The SchemaId corresponding to type <see cref="T"/>, or null if 
        ///     the serializer has yet to be invoked.
        /// </summary>
        public int? SchemaId { get; private set; } = null;

        /// <summary>
        ///	    The <see cref="ISchemaRegistryClient"/> instance used for communication
        ///	    with Confluent Schema Registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegistryClient { get; private set; }

        /// <summary>
        ///     The default initial size (in bytes) of buffers used for message 
        ///     serialization.
        /// </summary>
        public const int DefaultInitialBufferSize = 128;

        /// <summary>
        ///     True if the serializer will attempt to auto-register un-recognized schemas
        ///     with Confluent Schema Registry, false if not.
        /// </summary>
        public bool AutoRegisterSchema { get; private set; } = true;

        /// <summary>
        ///     Initial size (in bytes) of the buffer used for message serialization.
        /// </summary>
        /// <remarks>
        ///     Use a value high enough to avoid resizing of buffer, but small enough
        ///     to avoid excessive memory use. Inspect the size of byte array returned by
        ///     <see cref="Serialize(string, T)"/> to estimate an appropriate value.
        ///     Note: each call to serialize creates a new buffer.
        /// </remarks>
        public int InitialBufferSize { get; private set; } = DefaultInitialBufferSize;

        /// <summary>
        ///	    True if this serializer is used for serializing Kafka message keys,
        ///	    false if it is used for serializing Kafka message values.
        /// </summary>
        public bool IsKey { get; private set; }

        /// <summary>
        ///     The avro schema used to write values of type <see cref="T"/>
        /// </summary>
        public Avro.Schema WriterSchema { get; private set; }

        private void Initialize()
        {
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
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET string type, however we don't for consistency
                // with the Java avro serializer.
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
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET byte[] type, however we don't for consistency
                // with the Java avro serializer.
                WriterSchema = Avro.Schema.Parse("bytes");
            }
            else
            {
                throw new ArgumentException(
                    $"{nameof(AvroSerializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }

            avroWriter = new SpecificWriter<T>(WriterSchema);
            writerSchemaString = WriterSchema.ToString();
        }

        /// <summary>
        ///     Initialize a new instance of AvroSerializer.
        ///     
        ///     When passed as a parameter to the Confluent.Kafka.Producer constructor,
        ///     the following configuration properties will be extracted from the producer's
        ///     configuration property collection:
        ///     
        ///     schema.registry.url (required) - A comma-separated list of URLs for schema registry 
        ///         instances that are used to register or lookup schemas.
        ///                           
        ///     schema.registry.connection.timeout.ms (default: 30000) - Timeout for requests to 
        ///         Confluent Schema Registry.
        ///     
        ///     schema.registry.max.cached.schemas (default: 1000) - The maximum number of schemas 
        ///         to cache locally.
        ///     
        ///     avro.serializer.buffer.bytes (default: 128) - Initial size (in bytes) of the buffer 
        ///         used for message serialization. Use a value high enough to avoid resizing 
        ///         the buffer, but small enough to avoid excessive memory use. Inspect the size of 
        ///         the byte array returned by the Serialize method to estimate an appropriate value. 
        ///         Note: each call to serialize creates a new buffer.
        ///     
        ///     avro.serializer.auto.register.schemas (default: true) - true if the serializer should 
        ///         attempt to auto-register unrecognized schemas with Confluent Schema Registry, 
        ///         false if not.
        /// </summary>
        /// <remarks>
        ///     An instance of CachedSchemaRegistryClient will be created and managed 
        ///     internally based on configuration properties extracted from the collection
        ///     passed into the Producer constructor.
        /// </remarks>
        public AvroSerializer() { }

        /// <summary>
        ///     Initiliaze a new instance of the AvroSerializer class.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///	    An instance of an implementation of ISchemaRegistryClient used for
        ///	    communication with Confluent Schema Registry.
        /// </param>
        /// <exception cref="InvalidOperationException">
        ///	    The generic type <see cref="T"/> is not supported.
        /// </exception>
        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient)
        {
            disposeClientOnDispose = false;
            SchemaRegistryClient = schemaRegistryClient;
        }

        /// <summary>
        ///     Serialize an instance of type <see cref="T"/> to a byte array in avro format. The serialized
        ///     data is preceeded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order). This call may block or throw 
        ///     on first use for a particular topic during schema registration.
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
            if (!topicsRegistered.Contains(topic))
            {
                string subject = IsKey
                    ? SchemaRegistryClient.ConstructKeySubjectName(topic)
                    : SchemaRegistryClient.ConstructValueSubjectName(topic);

                // first usage: register/get schema to check compatibility

                if (AutoRegisterSchema)
                {
                    SchemaId = SchemaRegistryClient.RegisterSchemaAsync(subject, writerSchemaString).Result;
                }
                else
                {
                    SchemaId = SchemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString).Result;
                }

                topicsRegistered.Add(topic);
            }
			
            using (var stream = new MemoryStream(InitialBufferSize))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(Constants.MagicByte);

                writer.Write(IPAddress.HostToNetworkOrder(SchemaId.Value));
                avroWriter.Write(data, new BinaryEncoder(stream));

                // TODO: maybe change the ISerializer interface so that this copy isn't necessary.
                return stream.ToArray();
            }
        }

        /// <include file='../../Confluent.Kafka/include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var keyOrValue = isKey ? "Key" : "Value";
            var srConfig = config.Where(item => item.Key.StartsWith("schema.registry."));
            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));
            IsKey = isKey;

            if (avroConfig.Count() != 0)
            {
                int? initialBufferSize = (int?)Utils.ExtractPropertyValue(config, isKey, InitialBufferSizePropertyName, "AvroSerializer", typeof(int));
                if (initialBufferSize != null) { InitialBufferSize = initialBufferSize.Value; }

                bool? autoRegisterSchema = (bool?)Utils.ExtractPropertyValue(config, isKey, AutoRegisterSchemaPropertyName, "AvroSerializer", typeof(bool));
                if (autoRegisterSchema != null) { AutoRegisterSchema = autoRegisterSchema.Value; }

                foreach (var property in avroConfig)
                {
                    if (property.Key != AutoRegisterSchemaPropertyName && property.Key != InitialBufferSizePropertyName)
                    {
                        throw new ArgumentException($"{keyOrValue} AvroSerializer: unexpected configuration parameter {property.Key}");
                    }
                }
            }

            if (srConfig.Count() != 0)
            {
                if (SchemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroSerializer schema registry client was configured via both the constructor and configuration parameters.");
                }
 
                disposeClientOnDispose = true;
                SchemaRegistryClient = new CachedSchemaRegistryClient(config);
            }

            if (SchemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroSerializer schema registry client was not supplied or configured.");
            }

            Initialize();
            return config.Where(item => !item.Key.StartsWith("schema.registry.") && !item.Key.StartsWith("avro."));
        }

        /// <summary>
        ///     Releases any unmanaged resources owned by the serializer.
        /// </summary>
        public void Dispose() 
        {
            if (disposeClientOnDispose)
            {
                SchemaRegistryClient.Dispose();
            }
        }
    }
}
