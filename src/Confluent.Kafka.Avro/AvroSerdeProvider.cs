using System;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Confluent.Kafka.Serialization;
using Avro.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Manages the lifetime of a connection to schema registry,
    ///     and creation of serializers / deserializers that make use
    ///     of this connection.
    /// </summary>
    public class AvroSerdeProvider : IDisposable
    {
        AvroSerdeProviderConfig config;
        ISchemaRegistryClient schemaRegistryClient;
        
        /// <summary>
        ///     Initializes a new instance of the AvroSerdeProvider class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public AvroSerdeProvider(AvroSerdeProviderConfig config)
        {
            this.config = config;
            schemaRegistryClient = new CachedSchemaRegistryClient(config);
        }

        /// <summary>
        ///     Create a new avro deserializer. Use this deserializer with 
        ///     GenericRecord, types generated using the avrogen.exe tool or
        ///     one of the following primitive types: int, long, float,
        ///     double, boolean, string, byte[].
        /// </summary>
        /// <remarks>
        ///     Serialization format:
        ///       byte 0:           Magic byte use to identify the protocol format.
        ///       bytes 1-4:        Unique global id of the avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
        ///       following bytes:  The serialized data.
        /// </remarks>
        public IDeserializer<T> CreateDeserializer<T>()
        {
            var deserializer = new AvroDeserializer<T>(schemaRegistryClient);
            deserializer.Configure(config, false);
            return deserializer;
        }

        /// <summary>
        ///     Create a new avro serializer for message keys. Use this serializer
        ///     with GenericRecord, types generated using the avrogen.exe tool or
        ///     one of the following primitive types: int, long, float, double, 
        ///     boolean, string, byte[].
        /// </summary>
        public ISerializer<T> CreateKeySerializer<T>()
        {
            var serializer = new AvroSerializer<T>(schemaRegistryClient);
            serializer.Configure(config, true);
            return serializer;
        }
        
        /// <summary>
        ///     Create a new avro serializer for message values. Use this serializer
        ///     with GenericRecord, types generated using the avrogen.exe tool or
        ///     one of the following primitive types: int, long, float, double, 
        ///     boolean, string, byte[].
        /// </summary>
        public ISerializer<T> CreateValueSerializer<T>()
        {
            var serializer = new AvroSerializer<T>(schemaRegistryClient);
            serializer.Configure(config, false);
            return serializer;
        }

        /// <summary>
        ///     Releases all resources owned by this object.
        /// </summary>
        public void Dispose()
        {
            schemaRegistryClient.Dispose();
        }
    }
}

