using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
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

        private static IDictionary<ConfluentKafkaTraceEvent, string> ConfluentKafkaTraceEventDic => Enum.GetNames(typeof(ConfluentKafkaTraceEvent)).ToDictionary(x => (ConfluentKafkaTraceEvent)Enum.Parse(typeof(ConfluentKafkaTraceEvent), x));
        internal static void TraceEvent(ConfluentKafkaTraceEvent confluentKafkaTraceEvent, params object[] args)
        {
#if !NETSTANDARD1_3
            Source.TraceEvent(TraceEventType.Information, (int)confluentKafkaTraceEvent, ConfluentKafkaTraceEventDic[confluentKafkaTraceEvent], args);
#endif
        }
    }
}
