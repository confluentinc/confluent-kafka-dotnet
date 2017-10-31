namespace Confluent.Kafka.SchemaRegistry
{
    /// <summary>
    ///     Enumerates the two different types of subject associated with the two
    ///     components of a Kafka message (key and value).
    /// </summary>
    public enum SubjectType
    {
        /// <summary>
        ///     Message key
        /// </summary>
        Key = 0,

        /// <summary>
        ///     Message value
        /// </summary>
        Value = 1
    }
}
