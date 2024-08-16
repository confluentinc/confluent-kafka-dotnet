namespace Confluent.Kafka.TestsCommon;

using System;
using System.Collections.Generic;
using Confluent.Kafka;

public class TestConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
{
    public TestConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config) :
        base(EditConfig(config))
    {
        SetLogHandler((_, m) => Console.WriteLine(m.Message));
    }

    private static IEnumerable<KeyValuePair<string, string>> EditConfig(
        IEnumerable<KeyValuePair<string, string>> config)
    {
        var consumerConfig = new ConsumerConfig(
            new Dictionary<string, string>(config))
        { };

        var groupProtocol = TestConsumerGroupProtocol.GroupProtocol();
        if (groupProtocol != null)
        {
            consumerConfig.GroupProtocol = groupProtocol == "classic" ?
                GroupProtocol.Classic :
                GroupProtocol.Consumer;
        }


        return consumerConfig;
    }
}
