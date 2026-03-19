using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests.Rest.Authentication
{
    public class AzureIMDSBearerAuthenticationHeaderValueProviderTests
    {
        private int maxRetries = 3;
        private int retriesWaitMs = 1000;
        private int retriesMaxWaitMs = 5000;
        private string tokenEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token?resource=foo&client_id=bar&api-version=2018-02-01";
        private string logicalCluster = "lsrc-1234";
        private string identityPool = "pool-abcd";

        private HttpClient SetupFakeHttpClient(int expiry)
        {
            var fakeJson = $@"{{
                ""access_token"": ""test-imds-token"",
                ""expires_in"": {expiry}
            }}";

            var fakeResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(fakeJson, Encoding.UTF8, "application/json")
            };

            var fakeHandler = new FakeHttpMessageHandler(fakeResponse);
            return new HttpClient(fakeHandler);
        }

        private HttpClient SetupFailingHttpClient()
        {
            var fakeResponse = new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new StringContent("Internal Server Error")
            };

            var fakeHandler = new FakeHttpMessageHandler(fakeResponse);
            return new HttpClient(fakeHandler);
        }

        [Fact]
        public async Task ProviderInitializedOrExpired()
        {
            var httpClient = SetupFakeHttpClient(3600);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.True(provider.NeedsInitOrRefresh());
            await provider.InitOrRefreshAsync();
            Assert.False(provider.NeedsInitOrRefresh());
        }

        [Fact]
        public async Task GetAuthenticationHeader()
        {
            var httpClient = SetupFakeHttpClient(3600);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            await provider.InitOrRefreshAsync();
            var header = provider.GetAuthenticationHeader();
            Assert.Equal("Bearer test-imds-token", header.ToString());
        }

        [Fact]
        public void GetAuthenticationHeaderThrowsBeforeInit()
        {
            var httpClient = SetupFakeHttpClient(3600);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Throws<InvalidOperationException>(() => provider.GetAuthenticationHeader());
        }

        [Fact]
        public async Task CheckTokenExpiration()
        {
            var httpClient = SetupFakeHttpClient(0);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            await provider.InitOrRefreshAsync();
            Assert.True(provider.NeedsInitOrRefresh());
        }

        [Fact]
        public void GetLogicalClusterAndIdentityPool()
        {
            var httpClient = SetupFakeHttpClient(3600);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);
            Assert.Equal(logicalCluster, provider.GetLogicalCluster());
            Assert.Equal(identityPool, provider.GetIdentityPool());
        }

        [Fact]
        public async Task RequestUsesGetMethodAndMetadataHeader()
        {
            HttpMethod capturedMethod = null;
            bool hasMetadataHeader = false;

            var fakeJson = @"{""access_token"": ""token"", ""expires_in"": 3600}";
            var handler = new CapturingHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(fakeJson, Encoding.UTF8, "application/json")
                },
                request =>
                {
                    capturedMethod = request.Method;
                    hasMetadataHeader = request.Headers.Contains("Metadata")
                        && request.Headers.GetValues("Metadata").GetEnumerator().MoveNext();
                });

            var httpClient = new HttpClient(handler);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);

            await provider.InitOrRefreshAsync();

            Assert.Equal(HttpMethod.Get, capturedMethod);
            Assert.True(hasMetadataHeader);
        }

        [Fact]
        public async Task RetriesExhaustedThrowsException()
        {
            var httpClient = SetupFailingHttpClient();
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpoint, logicalCluster, identityPool,
                maxRetries: 0, retriesWaitMs: 10, retriesMaxWaitMs: 10);
            var ex = await Assert.ThrowsAsync<Exception>(() => provider.InitOrRefreshAsync());
            Assert.Contains("Failed to fetch token from server", ex.Message);
        }

        [Theory]
        [InlineData(
            "http://169.254.169.254/metadata/identity/oauth2/token?resource=api%3A%2F%2Ftest&client_id=abc&api-version=2018-02-01",
            "http://169.254.169.254/metadata/identity/oauth2/token?resource=api%3A%2F%2Ftest&client_id=abc&api-version=2018-02-01")]
        [InlineData(
            "https://custom.endpoint.com/token?resource=foo&client_id=bar",
            "https://custom.endpoint.com/token?resource=foo&client_id=bar")]
        public async Task RequestUsesCorrectTokenEndpointUrl(string tokenEndpointUrl, string expectedUrl)
        {
            Uri capturedUri = null;

            var fakeJson = @"{""access_token"": ""token"", ""expires_in"": 3600}";
            var handler = new CapturingHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(fakeJson, Encoding.UTF8, "application/json")
                },
                request =>
                {
                    capturedUri = request.RequestUri;
                });

            var httpClient = new HttpClient(handler);
            var provider = new AzureIMDSBearerAuthenticationHeaderValueProvider(
                httpClient, tokenEndpointUrl, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);

            await provider.InitOrRefreshAsync();

            Assert.Equal(expectedUrl, capturedUri.ToString());
        }

        [Fact]
        public void TokenEndpointUrlBuiltWithQueryParameters()
        {
            var baseUrl = "http://169.254.169.254/metadata/identity/oauth2/token";
            var query = "resource=api%3A%2F%2Ftest&client_id=abc&api-version=2018-02-01";

            var builder = new UriBuilder(new Uri(baseUrl));
            builder.Query = query;
            builder.Fragment = "";
            var result = builder.Uri.ToString();

            Assert.Equal(
                "http://169.254.169.254/metadata/identity/oauth2/token?resource=api%3A%2F%2Ftest&client_id=abc&api-version=2018-02-01",
                result);
        }

        [Fact]
        public void TokenEndpointUrlOverrideWithQueryParameters()
        {
            var customUrl = "https://custom.endpoint.com/token";
            var query = "resource=foo&client_id=bar";

            var builder = new UriBuilder(new Uri(customUrl));
            builder.Query = query;
            builder.Fragment = "";
            var result = builder.Uri.ToString();

            Assert.Equal("https://custom.endpoint.com/token?resource=foo&client_id=bar", result);
        }

    }

    public class CapturingHttpMessageHandler : HttpMessageHandler
    {
        private readonly HttpResponseMessage _fakeResponse;
        private readonly Action<HttpRequestMessage> _onSend;

        public CapturingHttpMessageHandler(HttpResponseMessage fakeResponse, Action<HttpRequestMessage> onSend)
        {
            _fakeResponse = fakeResponse;
            _onSend = onSend;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _onSend?.Invoke(request);
            return Task.FromResult(_fakeResponse);
        }
    }
}
