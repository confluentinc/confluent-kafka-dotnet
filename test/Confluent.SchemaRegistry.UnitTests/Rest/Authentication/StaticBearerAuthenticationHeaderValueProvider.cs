using System.Threading.Tasks;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests.Rest.Authentication
{
    public class StaticBearerAuthenticationHeaderValueProviderTests
    {
        private string token = "test-token";
        private string logicalCluster = "lsrc-1234";
        private string identityPool = "pool-abcd";

        [Fact]
        public async Task ProviderInitializedOrExpired()
        {
            var provider = new StaticBearerAuthenticationHeaderValueProvider(token, logicalCluster, identityPool);
            Assert.False(provider.NeedsInitOrRefresh());
            await provider.InitOrRefreshAsync();
            Assert.False(provider.NeedsInitOrRefresh());
        }

        [Fact]
        public void GetAuthenticationHeader() 
        {
            var provider = new StaticBearerAuthenticationHeaderValueProvider(token, logicalCluster, identityPool);
            var header = provider.GetAuthenticationHeader();
            Assert.Equal("Bearer test-token", header.ToString());
        }

        [Fact]
        public void GetLogicalClusterAndIdentityPool()
        {
            var provider = new StaticBearerAuthenticationHeaderValueProvider(token, logicalCluster, identityPool);
            Assert.Equal(logicalCluster, provider.GetLogicalCluster());
            Assert.Equal(identityPool, provider.GetIdentityPool());
        }
    }
}