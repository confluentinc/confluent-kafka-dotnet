namespace Confluent.Kafka
{
    /// <summary>
    ///     A serializer for use with <see cref="Confluent.Kafka.Producer" />
    /// </summary>
    /// <param name="data">
    ///     The value to serialize.
    /// </param>
    /// <returns>
    ///     The serialized value.
    /// </returns>
    public delegate byte[] Serializer<T>(T data);
}