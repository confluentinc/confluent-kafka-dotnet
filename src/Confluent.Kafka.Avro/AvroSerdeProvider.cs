using System;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Confluent.Kafka.Serialization;
using Avro.Generic;


namespace Confluent.Kafka.Serialization
{
    public class AvroSerdeProvider : IDisposable
    {
        ISchemaRegistryClient schemaRegistryClient;
        
        public AvroSerdeProvider(IEnumerable<KeyValuePair<string, object>> config)
        {
            schemaRegistryClient = new CachedSchemaRegistryClient(config);
        }

        public Deserializer<T> CreateAvroDeserializer<T>()
        {
            return (topic, data, isNull) => 
            {
                var deserializerImpl = (typeof(T) == typeof(GenericRecord))
                    ? (IAvroDeserializerImpl<T>)new GenericDeserializerImpl(schemaRegistryClient)
                    : new SpecificDeserializerImpl<T>(schemaRegistryClient);
                
                return deserializerImpl.Deserialize(topic, data.ToArray());
            };
        }

        public Serializer<T> CreateAvroKeySerializer<T>()
        {
            return null;
        }
        
        public Serializer<T> CreateAvroValueSerializer<T>()
        {
            return null;
        }

        public void Dispose()
        {
            schemaRegistryClient.Dispose();
        }
    }
}
