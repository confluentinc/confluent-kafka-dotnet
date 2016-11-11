using System;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka
{
    /// <summary>
    /// Miscellaneous APIs for the RdKafka library itself.
    /// </summary>
    public static class Library
    {
        /// <summary>
        /// Returns the librdkafka version as integer.
        ///
        /// Interpreted as hex MM.mm.rr.xx:
        ///  - MM = Major
        ///  - mm = minor
        ///  - rr = revision
        ///  - xx = pre-release id (0xff is the final release)
        ///
        /// E.g.: 0x000901ff = 0.9.1
        /// </summary>
        public static int Version => (int) LibRdKafka.version();

        /// <summary>
        /// The librdkafka version as string.
        /// </summary>
        public static string VersionString =>
            Marshal.PtrToStringAnsi(LibRdKafka.version_str());

        /// <summary>
        /// List of the supported debug contexts.
        /// </summary>
        public static string[] DebugContexts =>
            Marshal.PtrToStringAnsi(LibRdKafka.get_debug_contexts()).Split(',');

        public static void SetLogLevel(int logLevel)
        {
            LibRdKafka.set_log_level(IntPtr.Zero, (IntPtr) logLevel);
        }

        /// <summary>
        /// Wait for all rdkafka objects to be destroyed.
        ///
        /// Returns if all kafka objects are now destroyed,
        /// or throws TimeoutException if the timeout was reached.
        ///
        /// Since RdKafka handle deletion is an async operation the
        /// WaitDestroyed() function can be used for applications where
        /// a clean shutdown is required.
        /// </summary>
        /// <exception cref="System.TimeoutException">Timeout was reached before all objects were destroyed.</exception>
        public static void WaitDestroyed(TimeSpan timeout)
        {
            if ((long) LibRdKafka.wait_destroyed((IntPtr) timeout.TotalMilliseconds) != 0)
            {
                throw new TimeoutException();
            }
        }
    }
}
