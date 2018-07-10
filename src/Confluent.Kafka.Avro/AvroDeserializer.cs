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
using Avro.Generic;
using Confluent.SchemaRegistry;
using System;
using System.Linq;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro deserializer. Use this deserializer with GenericRecord, types 
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

        private ISchemaRegistryClient schemaRegistryClient;

        private IAvroDeserializerImpl<T> deserializerImpl;

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
        public AvroDeserializer()
        {
            disposeClientOnDispose = true;
        }

        /// <summary>
        ///     Initiliaze a new AvroDeserializer instance.
        /// </summary>
        /// <param name="schemaRegisteryClient">
        ///     An instance of an implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        public AvroDeserializer(ISchemaRegistryClient schemaRegisteryClient)
        {
            disposeClientOnDispose = false;
            schemaRegistryClient = schemaRegisteryClient;
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/> from a byte array.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <param name="data">
        ///     A byte array containing the object serialized in the format produced
        ///     by <see cref="AvroSerializer{T}" />.
        /// </param>
        /// <param name="isNull">
        ///     True if the data is null, false otherwise.
        /// </param>
        /// <returns>
        ///     The deserialized <typeparamref name="T"/> value.
        /// </returns>
        public T Deserialize(string topic, ReadOnlySpan<byte> data, bool isNull)
        {
            if (deserializerImpl == null)
            {
                deserializerImpl = (typeof(T) == typeof(GenericRecord))
                    ? (IAvroDeserializerImpl<T>)new GenericDeserializerImpl(schemaRegistryClient)
                    : new SpecificDeserializerImpl<T>(schemaRegistryClient);
            }

            return deserializerImpl.Deserialize(topic, data.ToArray());
        }

        /// <include file='../Confluent.Kafka/include_docs.xml' path='API/Member[@name="IDeserializer_Configure"]/*' />
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
                if (schemaRegistryClient != null)
                {
                    throw new ArgumentException($"{keyOrValue} AvroDeserializer schema registry client was configured via both the constructor and configuration parameters.");
                }

                schemaRegistryClient = new CachedSchemaRegistryClient(config);
            }

            if (schemaRegistryClient == null)
            {
                throw new ArgumentException($"{keyOrValue} AvroDeserializer schema registry client was not supplied or configured.");
            }

            return config.Where(item => !item.Key.StartsWith("schema.registry.") && !item.Key.StartsWith("avro."));
        }

        /// <summary>
        ///     Releases any unmanaged resources owned by the deserializer.
        /// </summary>
        public void Dispose()
        {
            if (disposeClientOnDispose)
            {
                schemaRegistryClient.Dispose();
            }
        }

    }
}
