using System;
using System.Collections.Generic;
using System.Text;

namespace Confluent.Kafka.Internal.Diagnostic
{
    public enum ConfluentKafkaTraceEvent
    {
        None = 0,

        InitByProducerBuilder = 11,
        InitByDependentProducerBuilder = 12,
        InitByConsumerBuilder = 13,

        Produce_Start = 21,
        ProduceAsync_Start = 22,
        Produce_End = 23,
        ProduceAsync_End = 24,
        HandleDeliveryReport = 25,

        Consume_Start = 31,
        Consume_End = 32,

        Error = 500
    }
}
