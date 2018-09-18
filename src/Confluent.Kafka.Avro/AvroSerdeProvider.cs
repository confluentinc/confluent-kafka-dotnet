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
        IEnumerable<KeyValuePair<string, string>> config;
        ISchemaRegistryClient schemaRegistryClient;
        
        /// <summary>
        ///     Initializes a new instance of the AvroSerdeProvider class.
        /// </summary>
        /// <param name="config">
        ///     Configuration properties.
        /// </param>
        public AvroSerdeProvider(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.config = config;
            schemaRegistryClient = new CachedSchemaRegistryClient(config);
        }

        /// <summary>
        ///     Create a new avro deserializer generator. Use this with 
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
        public DeserializerGenerator<T> GetDeserializerGenerator<T>()
        {
            return (forKey) => 
            {
                var deserializer = new AvroDeserializer<T>(schemaRegistryClient);
                deserializer.Configure(config, forKey);
                return (string topic, ReadOnlySpan<byte> data, bool isNull) => deserializer.Deserialize(topic, data, isNull);
            };
        }

        /// <summary>
        ///     Create a new avro serializer generator. Use this with
        ///     GenericRecord, types generated using the avrogen.exe tool or
        ///     one of the following primitive types: int, long, float, double, 
        ///     boolean, string, byte[].
        /// </summary>
        public SerializerGenerator<T> GetSerializerGenerator<T>()
        {
            return (forKey) =>
            {
                var serializer = new AvroSerializer<T>(schemaRegistryClient);
                serializer.Configure(config, true);
                return (string topic, T data) => serializer.Serialize(topic, data);
            };
        }
        
        /// <summary>
        ///     Releases all resources owned by this object.
        /// </summary>
        public void Dispose() => schemaRegistryClient.Dispose();
    }
}
