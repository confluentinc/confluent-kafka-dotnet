namespace Confluent.Kafka.TestsCommon;

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

public class TestProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
{
    public TestProducerBuilder(IEnumerable<KeyValuePair<string, string>> config) :
        base(EditConfig(config))
    {
        // Not on s390x for now: a log handler can hang the CI testing process there sometimes.
        if (RuntimeInformation.ProcessArchitecture != Architecture.S390x)
        {
            SetLogHandler((_, m) => Console.WriteLine(m.Message));
        }
    }

    private static IEnumerable<KeyValuePair<string, string>> EditConfig(
        IEnumerable<KeyValuePair<string, string>> config)
    {
        var producerConfig = new ProducerConfig(
            new Dictionary<string, string>(config))
        {};
        return producerConfig;
    }
}