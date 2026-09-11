using System;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.IntegrationTests;

public partial class Tests
{
    [Theory, MemberData(nameof(KafkaParameters))]
    public async Task DisposeAsyncDoesNotThrow(string bootstrapServers)
    {
        var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        string topicName = $"{nameof(DisposeAsyncDoesNotThrow)}-{Guid.NewGuid()}";
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
            }
        });
        await adminClient.DeleteTopicsAsync(new[] { topicName });

        await adminClient.DisposeAsync();
    }
}