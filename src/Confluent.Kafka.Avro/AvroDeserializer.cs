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
using Confluent.Kafka.SchemaRegistry;
using System.Reflection;
using System;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro specific deserializer for deserializing to types 
    ///     generated with the avrogen.exe tool.
    /// </summary>
    public class AvroDeserializer<T> : IDeserializer<T>
    {
        // Serialization format:
        //   [0]:       magic byte (use to identify protocol format)
        //   [1-4]:     unique global id of avro schema used for write (as registered in schema registry), BIG ENDIAN
        //   following: data serialized with corresponding schema

        /// <summary>
        ///	    The client used to communicate with Confluent Schema Registry.
        /// </summary>
        public ISchemaRegistryClient SchemaRegistryClient { get; private set;  }

        private bool disposeClientOnDispose;

        /// <remarks>
        ///     A deserializer cache is maintained, so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> readerBySchemaIdBigEndian = new Dictionary<int, DatumReader<T>>();
        private object readerLock = new object();

        /// <summary>
        ///     The avro schema corresponding to type <see cref="T"/>
        /// </summary>
        public Avro.Schema ReaderSchema { get; private set; }

        private void Initialize(ISchemaRegistryClient schemaRegistryClient)
        {
            SchemaRegistryClient = schemaRegistryClient;

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
                throw new InvalidOperationException($"{nameof(AvroDeserializer<T>)} " +
                    "only accepts int, bool, double, string, float, long, byte[], " +
                    "ISpecificRecord subclass and SpecificFixed");
            }
        }

        /// <summary>
        ///     Initialize a new instance of AvroDeserializer. Relevant configuration properties
        ///     will be extracted from the collection passed into the consumer constructor.
        /// </summary>
        public AvroDeserializer() { }

        /// <summary>
        ///     Initiliaze a new AvroDeserializer instance.
        /// </summary>
        /// <param name="schemaRegisterClient">
        ///	    Client used for communication with Confluent's Schema Registry.
        ///	</param>
        /// <exception cref="InvalidOperationException">
        ///	    The generic type <see cref="T"/> is not supported.
        ///	</exception>
        public AvroDeserializer(ISchemaRegistryClient schemaRegisterClient)
        {
            disposeClientOnDispose = false;
            Initialize(schemaRegisterClient);
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
            // topic is not necessary for deserialization (or knowing if it's key or not)
            // we only care about the schema id.
            using (var stream = new MemoryStream(array))
            using (var reader = new BinaryReader(stream))
            {
                int magicByte = reader.ReadByte();
                if (magicByte != Constants.MagicByte)
                {
                    //may change in the future with new format
                    throw new InvalidDataException("magic byte should be 0");
                }
                int writerIdBigEndian = reader.ReadInt32();

                DatumReader<T> datumReader = null;
                // TODO: A r/w lock would be better, but the improvement would be negligible here.
                lock (readerLock)
                {
                    readerBySchemaIdBigEndian.TryGetValue(writerIdBigEndian, out datumReader);
                }
                if (datumReader == null)
                {
                    int witerId = IPAddress.NetworkToHostOrder(writerIdBigEndian);
                    string writerSchemaJson = SchemaRegistryClient.GetSchemaAsync(witerId).Result;
                    var writerSchema = Avro.Schema.Parse(writerSchemaJson);

                    // can be of multiple type: Record, Primitive...
                    // we don't read against a given schema, so writer and reader schema are same
                    datumReader = new SpecificReader<T>(writerSchema, ReaderSchema);
                    lock (readerLock)
                    {
                        readerBySchemaIdBigEndian[writerIdBigEndian] = datumReader;
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

            if (srConfig.Count() != 0)
            {
                if (SchemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroDeserializer was configured using both constructor arguments and configuration parameters.");
                }

                disposeClientOnDispose = true;
                Initialize(new CachedSchemaRegistryClient(config));

                return config.Where(item => !item.Key.StartsWith("schema.registry."));
            }

            if (SchemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroDeserializer was not configured.");
            }

            return config;
        }

        /// <summary>
        ///     Releases any unmanaged resources owned by the deserializer.
        /// </summary>
        public void Dispose() 
        {
            if (disposeClientOnDispose)
            {
                this.SchemaRegistryClient.Dispose();
            }
        }
    }
}
