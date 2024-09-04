namespace Confluent.Kafka.TestsCommon;

using System;
using System.Collections.Generic;

public class TestProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
{
    public TestProducerBuilder(IEnumerable<KeyValuePair<string, string>> config) :
        base(EditConfig(config))
    {
        SetLogHandler((_, m) => Console.WriteLine(m.Message));
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