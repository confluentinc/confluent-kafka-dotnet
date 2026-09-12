using System;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a serializer for use with <see cref="Confluent.Kafka.Producer{TKey,TValue}" />.
    /// </summary>
    public interface ISegmentSerializer<T>
    {
        /// <summary>
        ///     Serialize the key or value of a <see cref="Message{TKey,TValue}" />
        ///     instance.
        /// </summary>
        /// <param name="data">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="ArraySegment{T}"/> containing the serialized value.
        /// </returns>
        ArraySegment<byte> Serialize(T data, SerializationContext context);
    }
}
