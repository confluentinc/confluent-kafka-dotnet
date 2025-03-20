using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Runtime.ConstrainedExecution;

namespace Confluent.SchemaRegistry
{
    class BearerToken 
    {   
        [JsonProperty("access_token")]
        public string bearerToken { get; set; }
        [JsonProperty("token_type")]
        public string tokenType { get; set; }
        [JsonProperty("expires_in")]
        public int expiresIn { get; set; }
        [JsonProperty("scope")]
        public string scope { get; set; }
    }

    public class BearerAuthenticationHeaderValueProvider : IAuthenticationBearerHeaderValueProvider, IDisposable
    {
        private readonly string clientId;
        private readonly string clientSecret;
        private readonly string scope;
        private readonly string tokenEndpoint;
        private readonly string logicalCluster;
        private readonly string identityPool;
        private readonly int maxRetries;
        private readonly int retriesWaitMs;
        private readonly int retriesMaxWaitMs;
        private BearerToken token;
        private const float tokenExpiryThreshold = 0.8f;
        private long tokenExpiryTime;
        public BearerAuthenticationHeaderValueProvider(string clientId, string clientSecret, string scope, string tokenEndpoint, 
            string logicalCluster, string identityPool, int maxRetries, int retriesWaitMs, int retriesMaxWaitMs)
        {
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.scope = scope;
            this.tokenEndpoint = tokenEndpoint;
            this.logicalCluster = logicalCluster;
            this.identityPool = identityPool;
            this.maxRetries = maxRetries;
            this.retriesWaitMs = retriesWaitMs;
            this.retriesMaxWaitMs = retriesMaxWaitMs;
        }

        public async Task InitializeAsync()
        {
            await GenerateToken();
            CalculateTokenExpiryTime();
        }

        public bool ProviderInitializedOrNotExpired()
        {
            return token != null && DateTimeOffset.UtcNow.ToUnixTimeSeconds() < tokenExpiryTime;

        }

        private HttpRequestMessage CreateTokenRequest()
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
            
            request.Content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("client_id", clientId),
                new KeyValuePair<string, string>("client_secret", clientSecret),
                new KeyValuePair<string, string>("scope", scope)
            });

            return request;
        }

        private async Task GenerateToken()
        {
            using (var client = new HttpClient())
            {
                var request = CreateTokenRequest();

                for (int i = 0; i < maxRetries + 1; i++){
                    try 
                    {
                        var response = await client.SendAsync(request).ConfigureAwait(continueOnCapturedContext: false);
                        response.EnsureSuccessStatusCode();
                        var tokenResponse = await response.Content.ReadAsStringAsync();
                        token = JObject.Parse(tokenResponse).ToObject<BearerToken>(JsonSerializer.Create());
                        return;
                    }
                    catch (Exception e)
                    {
                        if (i == maxRetries)
                        {
                            throw new Exception("Failed to fetch token from server: " + e.Message);
                        }
                        await Task.Delay(RetryUtility.CalculateRetryDelay(retriesWaitMs, retriesMaxWaitMs, i));
                    }
                }
            }
        }

        private void CalculateTokenExpiryTime()
        {
            tokenExpiryTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + (int)(token.expiresIn * tokenExpiryThreshold);
        }

        public AuthenticationHeaderValue GetAuthenticationHeader() => new AuthenticationHeaderValue("Bearer", this.token.bearerToken);

        public string GetLogicalCluster() => this.logicalCluster;

        public string GetIdentityPool() => this.identityPool;

        public void Dispose()
        {
            this.token = null;
        }
    }
}