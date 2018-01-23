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

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Confluent.SchemaRegistry;
using System.Reflection;
using System;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro specific deserializer. Use this deserializer with types 
    ///     generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroDeserializer<T> : IDeserializer<T>
    {
        private bool disposeClientOnDispose;

        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> datumReaderBySchemaId = new Dictionary<int, DatumReader<T>>();
        private object datumReaderLock = new object();

        /// <summary>
        ///     The avro schema used to read values of type <see cref="T"/>
        /// </summary>
        public Avro.Schema ReaderSchema { get; private set; }

        /// <summary>
        ///	    The client used to communicate with Confluent Schema Registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegistryClient { get; private set; }

        private void Initialize()
        {
            if (typeof(ISpecificRecord).IsAssignableFrom(typeof(T)) || typeof(T).IsSubclassOf(typeof(SpecificFixed)))
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
            else
            {
                throw new ArgumentException(
                    $"{nameof(AvroDeserializer<T>)} " +
                    "only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }
        }

        /// <summary>
        ///     Initialize a new instance of AvroDeserializer.
        ///     
        ///     When passed as a parameter to the Confluent.Kafka.Consumer constructor,
        ///     the following configuration properties will be extracted from the consumer's
        ///     configuration property collection:
        ///     
        ///     schema.registry.url (required) - A comma-separated list of URLs for schema registry 
        ///         instances that can be used to register or lookup schemas.
        ///                           
        ///     schema.registry.connection.timeout.ms (default: 30000) - Timeout for requests to 
        ///         Confluent Schema Registry.
        ///     
        ///     schema.registry.max.cached.schemas (default: 1000) - The maximum number of schemas 
        ///         to cache locally.
        /// </summary>
        /// <remarks>
        ///     An instance of CachedSchemaRegistryClient will be created and managed 
        ///     internally based on configuration properties extracted from the collection
        ///     passed into the Consumer constructor.
        /// </remarks>
        public AvroDeserializer() { }

        /// <summary>
        ///     Initiliaze a new AvroDeserializer instance.
        /// </summary>
        /// <param name="schemaRegisteryClient">
        ///     An instance of an implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <exception cref="InvalidOperationException">
        ///	    The generic type <see cref="T"/> is not supported.
        /// </exception>
        public AvroDeserializer(ISchemaRegistryClient schemaRegisteryClient)
        {
            disposeClientOnDispose = false;
            SchemaRegistryClient = schemaRegisteryClient;
        }

        /// <summary>
        ///     Deserialize an object of type <see cref="T"/> from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="array">
        ///     A byte array containing the object serialized in the format produced
        ///     by <see cref="AvroSerializer" />.
        /// </param>
        /// <returns>
        ///     The deserialized <see cref="T"/> value.
        /// </returns>
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
                var writerIdBigEndian = reader.ReadInt32();
                var writerId = IPAddress.NetworkToHostOrder(writerIdBigEndian);

                DatumReader<T> datumReader = null;

                lock (datumReaderLock)
                {
                    datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                    if (datumReader == null)
                    {
                        // it's a good idea to retain the lock over this blocking call since there
                        // may be concurrent deserialize calls for the same T, and in that case it's
                        // best not to hit Schema Registry more than once for the same information.
                        var writerSchemaJson = SchemaRegistryClient.GetSchemaAsync(writerId).Result;
                        var writerSchema = Avro.Schema.Parse(writerSchemaJson);

                        datumReader = new SpecificReader<T>(writerSchema, ReaderSchema);
                        datumReaderBySchemaId[writerId] = datumReader;
                    }
                }
                
                return datumReader.Read(default(T), new BinaryDecoder(stream));
            }
        }

        /// <include file='../../Confluent.Kafka/include_docs.xml' path='API/Member[@name="IDeserializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            var keyOrValue = isKey ? "Key" : "Value";
            var srConfig = config.Where(item => item.Key.StartsWith("schema.registry."));
            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));

            if (avroConfig.Count() != 0)
            {
                throw new ArgumentException($"{keyOrValue} AvroDeserializer: unexpected configuration parameter {avroConfig.First().Key}");
            }

            if (srConfig.Count() != 0)
            {
                if (SchemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroDeserializer schema registry client was configured via both the constructor and configuration parameters.");
                }

                disposeClientOnDispose = true;
                SchemaRegistryClient = new CachedSchemaRegistryClient(config);
            }

            if (SchemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroDeserializer schema registry client was not supplied or configured.");
            }

            Initialize();
            return config.Where(item => !item.Key.StartsWith("schema.registry.") && !item.Key.StartsWith("avro."));
        }

        /// <summary>
        ///     Releases any unmanaged resources owned by the deserializer.
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
