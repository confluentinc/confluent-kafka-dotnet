
using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;


namespace Confluent.SchemaRegistry.Serdes
{
    public class AvroDeserializer<T> : IDeserializer<T>
    {
        AsyncAvroDeserializer<T> asyncDeserializer;

        public AvroDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            asyncDeserializer = new AsyncAvroDeserializer<T>(schemaRegistryClient, config);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => asyncDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(data.ToArray()), isNull, context)
                .ConfigureAwait(continueOnCapturedContext: false)
                .GetAwaiter()
                .GetResult();
    }
}
