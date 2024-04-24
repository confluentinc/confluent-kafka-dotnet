namespace Confluent.Kafka.TestsCommon;

using System;


public class TestConsumerGroupProtocol
{
    public static bool IsClassic()
    {
        var consumerGroupProtocol = GroupProtocol();
        return consumerGroupProtocol == null ||
            consumerGroupProtocol == "classic";
    }

    public static string GroupProtocol()
    {
        return Environment.GetEnvironmentVariable(
            "TEST_CONSUMER_GROUP_PROTOCOL");
    }
}