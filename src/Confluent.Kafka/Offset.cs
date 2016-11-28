
namespace Confluent.Kafka
{

    // TODO: Is it possible to get rid of this? have Seek methods instead?
    // Maybe various methods could be overloaded to take enum or int?

    public static class Offset
    {
        /// <summary>
        ///     Start consuming from beginning of kafka partition queue: oldest msg
        /// </summary>
        public const long Beginning = -2;

        /// <summary>
        ///     Start consuming from end of kafka partition queue: next msg
        /// </summary>
        public const long End = -1;

        /// <summary>
        ///     Start consuming from offset retrieved from offset store
        /// </summary>
        public const long Stored = -1000;

        /// <summary>
        ///     Invalid offset
        /// </summary>
        public const long Invalid = -1001;
    }

}
