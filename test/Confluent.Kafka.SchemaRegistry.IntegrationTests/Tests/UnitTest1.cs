using System;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void Test1(string server)
        {
            var sr = new CachedSchemaRegistryClient(server);
            
        }
    }
}
