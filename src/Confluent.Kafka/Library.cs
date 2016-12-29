using Confluent.Kafka.Internal;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Methods that relate to the RdKafka library itself.
    ///     (do not require a Producer or Consumer broker connection).
    /// </summary>
    public static class Library
    {
        /// <summary>
        ///     Returns the librdkafka version as an integer.
        ///
        ///     Interpreted as hex MM.mm.rr.xx:
        ///         - MM = Major
        ///         - mm = minor
        ///         - rr = revision
        ///         - xx = pre-release id (0xff is the final release)
        ///
        ///     E.g.: 0x000901ff = 0.9.1
        /// </summary>
        public static int Version
            => (int) LibRdKafka.version();

        /// <summary>
        ///     The librdkafka version as string.
        /// </summary>
        public static string VersionString
            => Util.Marshal.PtrToStringUTF8(LibRdKafka.version_str());

        /// <summary>
        ///     List of the supported debug contexts.
        /// </summary>
        public static string[] DebugContexts
            => Util.Marshal.PtrToStringUTF8(LibRdKafka.get_debug_contexts()).Split(',');

    }
}
