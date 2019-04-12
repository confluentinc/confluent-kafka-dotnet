using System;

namespace Confluent.Kafka
{
    public class SyncOverAsyncSerializer<T> : ISerializer<T>
    {
        private IAsyncSerializer<T> asyncSerializer { get; }

        public SyncOverAsyncSerializer(IAsyncSerializer<T> asyncSerializer)
        {
            this.asyncSerializer = asyncSerializer;
        }

        public byte[] Serialize(T data, SerializationContext context)
            => asyncSerializer.SerializeAsync(data, context)
                .ConfigureAwait(continueOnCapturedContext: false)
                .GetAwaiter()
                .GetResult();
    }
}
