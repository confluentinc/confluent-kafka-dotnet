using System.Threading.Tasks;
using Xunit;

namespace Confluent.Kafka.UnitTests;

public class DisposeAsync
{
    [Fact]
    public async Task TestDisposeAsyncDoesNotThrow()
    {
        var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:90922" }).Build();

        await adminClient.DisposeAsync();
    }
}