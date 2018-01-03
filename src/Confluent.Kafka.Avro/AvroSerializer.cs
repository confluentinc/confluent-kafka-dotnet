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
using Confluent.Kafka.SchemaRegistry;
using System.Reflection;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro specific serializer for serializing types generated using
    ///     the avrogen.exe tool.
    /// </summary>
    public class AvroSerializer<T> : ISerializer<T>
    {
        // Serialization format:
        //   [0]:       magic byte (use to identify protocol format)
        //   [1-4]:     unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        //   following: data serialized with corresponding schema

        private SpecificWriter<T> avroWriter;

        /// <summary>
        ///     The SchemaId corresponding to type <see cref="T"/>, or null if 
        ///     the serializer has yet to be invoked.
        /// </summary>
        public int? SchemaId { get; private set; } = null;

        /// <summary>
        ///     The schema id in big-endian (network) byte ordering.
        /// </summary>
        private int schemaIdBigEndian;

        private HashSet<string> topicsRegistred = new HashSet<string>();

		/// <summary>
        ///		The client used to communicate with Confluent Schema Registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegistryClient { get; private set; }

        /// <summary>
        ///     The default initial size (in bytes) of the buffer used to serialize 
        ///     messages to.
        /// </summary>
        public const int DefaultInitialBufferSize = 128;

        private bool disposeClientOnDispose;

        private const string InitialBufferSizePropertyName = "schema.registry.avro.buffer.size";

        /// <summary>
        ///		True if this serializer is used for serializing Kafka message keys,
        ///		false if it is used for serializing Kafka message values.
        /// </summary>
        public bool IsKey { get; private set; }

        /// <summary>
        ///     The avro schema corresponding to type <see cref="T"/>
        /// </summary>
        public Avro.Schema WriterSchema { get; private set; }

        /// <summary>
        ///     Initial size (in bytes) of the buffer used to serialize messages to.
        /// </summary>
        /// <remarks>
        ///     Use a value high enough to avoid resizing of buffer, but small enough
        ///     to avoid excessive memory use. Inspect the size of byte array returned by
        ///     <see cref="Serialize(string, T)"/> to estimate an appropriate value.
        ///     Note: each call to serialize creates a new buffer.
        /// </remarks>
        public int InitialBufferSize { get; private set; }

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
                throw new InvalidOperationException($"{nameof(AvroSerializer<T>)} " +
                    "only accepts int, bool, double, string, float, long, byte[], " +
                    "ISpecificRecord subclass and SpecificFixed");
            }

            avroWriter = new SpecificWriter<T>(WriterSchema);
        }

        /// <summary>
        ///     Initialize a new AvroSerializer instance. Relevant configuration properties
        ///     will be extracted from the collection passed into the producer constructor.
        /// </summary>
        public AvroSerializer() { }

        /// <summary>
        ///     Initiliaze an AvroSerializer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///		A client used to communicate with Confluent schema registry.
        ///	</param>
        /// <exception cref="InvalidOperationException">
        ///		The generic type <see cref="T"/> is not supported.
        ///	</exception>
        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient)
        {
            disposeClientOnDispose = false;
            SchemaRegistryClient = schemaRegistryClient;
            InitialBufferSize = DefaultInitialBufferSize;
            Initialize();
        }

        /// <summary>
        ///     Initialize a new instance of AvroSerializer.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///		Client used for communication with Confluent's Schema Registry.
        ///	</param>
        /// <param name="initialBufferSize">
        ///     The initial size (in bytes) of the buffer used for serializing messages (note: 
        ///     each call to serialize creates a new buffer). You should specify a value high 
        ///     enough to avoid re-allocation, but not so high as to cause excessive memory use.
        /// </param>
        /// <exception cref="InvalidOperationException">
        ///		The generic type <see cref="T"/> is not supported.
        ///	</exception>
        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient, int initialBufferSize)
        {
            disposeClientOnDispose = false;
            SchemaRegistryClient = schemaRegistryClient;
            InitialBufferSize = initialBufferSize;
            Initialize();
        }

        /// <summary>
        ///     Serialize an instance of type <see cref="T"/> to a byte array in avro format. The serialized
        ///     data is preceeded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in the Confluent Schema Registry (4 bytes, network byte order). This call may block
        ///     or throw on first use during schema registration.
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
                string subject = IsKey
                    ? SchemaRegistryClient.ConstructKeySubjectName(topic)
                    : SchemaRegistryClient.ConstructValueSubjectName(topic);

                // schemaId could already be intialized through an other topic
                // this has no impact, as schemaId will be the same
                var w = WriterSchema.ToString();
                SchemaId = SchemaRegistryClient.RegisterAsync(subject, WriterSchema.ToString()).Result;
                // use big endian
                schemaIdBigEndian = IPAddress.NetworkToHostOrder(SchemaId.Value);
                topicsRegistred.Add(topic);
            }
			
            using (var stream = new MemoryStream(InitialBufferSize))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(Constants.MagicByte);

                writer.Write(schemaIdBigEndian);
                avroWriter.Write(data, new BinaryEncoder(stream));

                // TODO: change the ISerializer interface so that this copy isn't necessary.
                return stream.ToArray();
            }
        }

        /// <include file='../../Confluent.Kafka/include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var keyOrValue = isKey ? "Key" : "Value";
            var srConfig = config.Where(item => item.Key.StartsWith("schema.registry."));
            this.IsKey = isKey;

            if (srConfig.Count() != 0)
            {
                if (SchemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroSerializer was configured using both constructor arguments and configuration parameters.");
                }

                int bufferSize = DefaultInitialBufferSize;

                var bufferSizeProperties = config.Where(ci => ci.Key == InitialBufferSizePropertyName);
                if (bufferSizeProperties.Count() > 1)
                {
                    throw new ArgumentException($"{keyOrValue} AvroSerializer {InitialBufferSizePropertyName} configuration parameter was specified more than once.");
                }
                
                bufferSize = DefaultInitialBufferSize;
                if (bufferSizeProperties.Count() == 0)
                {
                    try
                    {
                        bufferSize = int.Parse(bufferSizeProperties.First().Value.ToString());
                    }
                    catch (Exception e)
                    {
                        throw new ArgumentException($"{keyOrValue} AvroSerializer {InitialBufferSizePropertyName} configuration parameter was incorrectly specified.", e);
                    }
                }
                
                disposeClientOnDispose = true;
                SchemaRegistryClient = new CachedSchemaRegistryClient(config);
                InitialBufferSize = bufferSize;
                Initialize();

                return config.Where(item => !item.Key.StartsWith("schema.registry."));
            }

            if (SchemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroSerializer was not configured.");
            }

            return config;
        }

        /// <summary>
        ///     Releases any unmanaged resources owned by the serializer.
        /// </summary>
        public void Dispose() 
        {
            if (this.disposeClientOnDispose)
            {
                this.SchemaRegistryClient.Dispose();
            }
        }
    }
}
