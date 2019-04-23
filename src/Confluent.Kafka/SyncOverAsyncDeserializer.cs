using System;

namespace Confluent.Kafka.SyncOverAsync
{
    /// <summary>
    ///     An adapter that allows an async deserializer
    ///     to be used where a sync deserializer is required.
    ///     In using this adapter, there are two potential
    ///     issues you should be aware of:
    /// 
    ///     1. If you are working in a single threaded
    ///        SynchronizationContext (for example, a
    ///        WindowsForms application), you must ensure
    ///        that all methods awaited by your deserializer
    ///        (at all levels) are configured to NOT 
    ///        continue on the captured context, otherwise
    ///        your application will deadlock. You do this
    ///        by calling .ConfigureAwait(false) on every
    ///        method awaited in your deserializer
    ///        implementation. If your deserializer makes use
    ///        of a library that does not do this, you
    ///        can get around this by calling await
    ///        Task.Run(() => ...) to force the library
    ///        method to execute in a SynchronizationContext
    ///        that is not single threaded. Note: all
    ///        Confluent async deserializers comply with the
    ///        above.
    ///  
    ///     2. In any application, there is potential
    ///        for a deadlock due to thread pool exhaustion.
    ///        This can happen because in order for an async
    ///        method to complete, a thread pool thread is
    ///        typically required. However, if all available
    ///        thread pool threads are in use waiting for the 
    ///        async methods to complete, there will be
    ///        no threads available to complete the tasks
    ///        (deadlock). Due to (a) the large default
    ///        number of thread pool threads in the modern
    ///        runtime and (b) the infrequent need for a
    ///        typical async deserializer to wait on an async
    ///        result (i.e. most deserializers will only
    ///        infrequently need to execute asynchronously),
    ///        this scenario should not commonly occur in
    ///        practice.
    /// </summary>
    public class SyncOverAsyncDeserializer<T> : IDeserializer<T>
    {
        private IAsyncDeserializer<T> asyncDeserializer { get; }

        /// <summary>
        ///     Initializes a new SyncOverAsyncDeserializer.
        /// </summary>
        public SyncOverAsyncDeserializer(IAsyncDeserializer<T> asyncDeserializer)
        {
            this.asyncDeserializer = asyncDeserializer;
        }

        /// <summary>
        ///     Deserialize a message key or value.
        /// </summary>
        /// <param name="data">
        ///     The data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     Whether or not the value is null.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize
        ///     operation.
        /// </param>
        /// <returns>
        ///     The deserialized value.
        /// </returns>
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => asyncDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(data.ToArray()), isNull, context)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();
    }

    /// <summary>
    ///     Extension methods related to SyncOverAsyncDeserializer.
    /// </summary>
    public static class SyncOverAsyncDeserializerExtensionMethods
    {
        /// <summary>
        ///     Create a sync deserializer by wrapping an async
        ///     one. For more information on the potential
        ///     pitfalls in doing this, refer to <see cref="Confluent.Kafka.SyncOverAsync.SyncOverAsyncDeserializer{T}" />.
        /// </summary>
        public static IDeserializer<T> AsSyncOverAsync<T>(this IAsyncDeserializer<T> asyncDeserializer)
            => new SyncOverAsyncDeserializer<T>(asyncDeserializer);
    }
}
