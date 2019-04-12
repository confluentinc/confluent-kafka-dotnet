using System;

namespace Confluent.Kafka
{
    public class SyncOverAsyncDeserializer<T> : IDeserializer<T>
    {
        private IAsyncDeserializer<T> asyncDeserializer { get; }

        public SyncOverAsyncDeserializer(IAsyncDeserializer<T> asyncDeserializer)
        {
            this.asyncDeserializer = asyncDeserializer;
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => asyncDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(data.ToArray()), isNull, context)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
    }
}
