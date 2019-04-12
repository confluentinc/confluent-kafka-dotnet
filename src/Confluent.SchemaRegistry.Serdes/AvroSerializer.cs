
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;


namespace Confluent.SchemaRegistry.Serdes
{
    public class AvroSerializer<T> : ISerializer<T>
    {
        AsyncAvroSerializer<T> asyncSerializer;

        public AvroSerializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            asyncSerializer = new AsyncAvroSerializer<T>(schemaRegistryClient, config);
        }

        public byte[] Serialize(T data, SerializationContext context)
            => asyncSerializer.SerializeAsync(data, context)
                .ConfigureAwait(continueOnCapturedContext: false)
                .GetAwaiter()
                .GetResult();
    }
}
