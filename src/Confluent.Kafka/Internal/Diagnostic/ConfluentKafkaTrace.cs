using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;

namespace Confluent.Kafka.Internal.Diagnostic
{
    public static class ConfluentKafkaTrace
    {
        public static long DriverCounter;
#if !NETSTANDARD1_3
        private static TraceSource Source = new TraceSource("ConfluentKafka");
        public static TraceListenerCollection Listeners => Source.Listeners;
        public static SourceSwitch Switch => Source.Switch;
#endif

        internal static void TraceEvent(ConfluentKafkaTraceEvent confluentKafkaTraceEvent, params object[] args)
        {
#if !NETSTANDARD1_3
            Source.TraceEvent(TraceEventType.Information, (int)confluentKafkaTraceEvent, confluentKafkaTraceEvent.ToString(), args);
#endif
        }
    }
}
