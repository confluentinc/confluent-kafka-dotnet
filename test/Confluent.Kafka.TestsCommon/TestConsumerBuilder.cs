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
            new Dictionary<string, string>(config)) {};

        var groupProtocol = TestConsumerGroupProtocol.GroupProtocol();
        if (groupProtocol != null)
        {
            consumerConfig.GroupProtocol = groupProtocol == "classic" ?
                GroupProtocol.Classic :
                GroupProtocol.Consumer;
        }

        if (consumerConfig.GroupProtocol == GroupProtocol.Consumer)
        {
            ISet<string> forbiddenProperties = new HashSet<string>
            {
                "partition.assignment.strategy",
                "session.timeout.ms",
                "heartbeat.interval.ms",
                "group.protocol.type"
            };
            var filteredConfig = new ConsumerConfig();
            foreach (var property in consumerConfig)
            {
                if (forbiddenProperties.Contains(property.Key))
                {
                    Console.WriteLine(
                        "Skipping setting forbidden configuration property \"" +
                        property.Key + 
                        "\" for CONSUMER protocol.");
                }
                else
                {
                    filteredConfig.Set(property.Key, property.Value);
                }
            }
            consumerConfig = filteredConfig;
        }

        
        return consumerConfig;
    }
}