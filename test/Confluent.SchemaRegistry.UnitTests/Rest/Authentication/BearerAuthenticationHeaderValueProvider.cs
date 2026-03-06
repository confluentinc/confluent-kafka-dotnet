using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests.Rest.Authentication
{

    public class FakeHttpMessageHandler : HttpMessageHandler
    {   
        private readonly HttpResponseMessage _fakeResponse;

        public FakeHttpMessageHandler(HttpResponseMessage fakeResponse)
        {
            _fakeResponse = fakeResponse;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(_fakeResponse);
        }
    }
    public class BearerAuthenticationHeaderValueProviderTests
    {
        private int maxRetries = 3;
        private int retriesWaitMs = 1000;
        private int retriesMaxWaitMs = 5000;
        private string clientId = "clientId";
        private string clientSecret = "clientSecret";
        private string scope = "scope";
        private string tokenEndpoint = "https://auth.example.com/oauth/token";
        private string logicalCluster = "lsrc-1234";
        private string identityPool = "pool-abcd";
        private HttpClient httpClient;

        private HttpClient SetupFakeHttpClient(int expiry)
        {
            var fakeJson = $@"{{
                ""access_token"": ""test-token"",
                ""token_type"": ""Bearer"",
                ""expires_in"": {expiry},
                ""scope"": ""schema_registry""
            }}";

            var fakeResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(fakeJson, Encoding.UTF8, "application/json")
            };

            var fakeHandler = new FakeHttpMessageHandler(fakeResponse);
            return new HttpClient(fakeHandler);
        }

        [Fact]
        public async Task ProviderInitializedOrExpired()
        {
            httpClient = SetupFakeHttpClient(3600);
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.True(provider.NeedsInitOrRefresh());
            await provider.InitOrRefreshAsync();
            Assert.False(provider.NeedsInitOrRefresh());
        }

        [Fact]
        public async Task GetAuthenticationHeader() 
        {
            httpClient = SetupFakeHttpClient(3600);
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            await provider.InitOrRefreshAsync();
            var header = provider.GetAuthenticationHeader();
            Assert.Equal("Bearer test-token", header.ToString());
        }

        [Fact]
        public void GetAuthenticationHeaderThrowsException()
        {
            httpClient = SetupFakeHttpClient(3600);
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Throws<InvalidOperationException>(() => provider.GetAuthenticationHeader());
        }

        [Fact]
        public async Task CheckTokenExpiration()
        {
            httpClient = SetupFakeHttpClient(0);
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            await provider.InitOrRefreshAsync();
            Assert.True(provider.NeedsInitOrRefresh());
        }

        [Fact]
        public void GetLogicalClusterAndIdentityPool()
        {
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Equal(logicalCluster, provider.GetLogicalCluster());
            Assert.Equal(identityPool, provider.GetIdentityPool());
        }

        [Fact]
        public void GetIdentityPool_CommaSeparatedList()
        {
            var commaSeparatedPools = "pool-1,pool-2,pool-3";
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, commaSeparatedPools, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Equal(commaSeparatedPools, provider.GetIdentityPool());
        }

        [Fact]
        public void GetIdentityPool_NullValue()
        {
            // Identity pool is optional for union of 
            var provider = new BearerAuthenticationHeaderValueProvider(
                httpClient, clientId, clientSecret, scope, tokenEndpoint, logicalCluster, null, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Null(provider.GetIdentityPool());
        }
    }
}