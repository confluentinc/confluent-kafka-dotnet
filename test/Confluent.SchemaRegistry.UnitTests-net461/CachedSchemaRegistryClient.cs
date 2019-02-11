using Confluent.SchemaRegistry;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests_net461
{
    
    public class CachedSchemaRegistryClientTests
    {
        [Fact]
        public void ConvertToStringInNet461DoesNotReturnEmptyStringForNullValues()
        {
            var schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig()
            {
               SchemaRegistryUrl = "http://irrelevanthost:80",
            });

            Assert.NotNull(schemaRegistryClient);
        }
    }
}
