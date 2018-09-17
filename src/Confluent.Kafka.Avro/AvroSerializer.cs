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
using Avro.Generic;
using Confluent.SchemaRegistry;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Avro serializer. Use this serializer with GenericRecord, types 
    ///     generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class AvroSerializer<T>
    {
        private bool autoRegisterSchema = true;
        private int initialBufferSize = DefaultInitialBufferSize;
        private bool isKeySerializer;
        private ISchemaRegistryClient schemaRegistryClient;

        private IAvroSerializerImpl<T> serializerImpl;


        /// <summary>
        ///     The default initial size (in bytes) of buffers used for message 
        ///     serialization.
        /// </summary>
        public const int DefaultInitialBufferSize = 1024;


        /// <summary>
        ///     Initiliaze a new instance of the AvroSerializer class.
        ///     When passed as a parameter to the Confluent.Kafka.Producer constructor,
        ///     the following configuration properties will be extracted from the producer's
        ///     configuration property collection:
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
        /// <param name="schemaRegistryClient">
        ///	    An instance of an implementation of ISchemaRegistryClient used for
        ///	    communication with Confluent Schema Registry.
        /// </param>
        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;
        }

        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a byte array in avro format. The serialized
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
            if (serializerImpl == null)
            {
                serializerImpl = typeof(T) == typeof(GenericRecord)
                    ? (IAvroSerializerImpl<T>)new GenericSerializerImpl(schemaRegistryClient, autoRegisterSchema, initialBufferSize, isKeySerializer)
                    : new SpecificSerializerImpl<T>(schemaRegistryClient, autoRegisterSchema, initialBufferSize, isKeySerializer);
            }

            return serializerImpl.Serialize(topic, data);
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Serialization.IDeserializer{T}.Configure(IEnumerable{KeyValuePair{string, string}}, bool)" />
        /// </summary>
        public IEnumerable<KeyValuePair<string, string>> Configure(IEnumerable<KeyValuePair<string, string>> config, bool isKey)
        {
            var keyOrValue = isKey ? "Key" : "Value";
            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));
            this.isKeySerializer = isKey;

            if (avroConfig.Count() != 0)
            {
                int? initialBufferSize = (int?)Utils.ExtractPropertyValue(config, isKey, ConfigPropertyNames.AvroSerializerBufferBytes, "AvroSerializer", typeof(int));
                if (initialBufferSize != null) { this.initialBufferSize = initialBufferSize.Value; }

                bool? autoRegisterSchema = (bool?)Utils.ExtractPropertyValue(config, isKey, ConfigPropertyNames.AvroSerializerAutoRegisterSchemas, "AvroSerializer", typeof(bool));
                if (autoRegisterSchema != null) { this.autoRegisterSchema = autoRegisterSchema.Value; }

                foreach (var property in avroConfig)
                {
                    if (property.Key != ConfigPropertyNames.AvroSerializerAutoRegisterSchemas && property.Key != ConfigPropertyNames.AvroSerializerBufferBytes)
                    {
                        throw new ArgumentException($"{keyOrValue} AvroSerializer: unexpected configuration parameter {property.Key}");
                    }
                }
            }

            return config.Where(item => !item.Key.StartsWith("avro."));
        }
    }
}
