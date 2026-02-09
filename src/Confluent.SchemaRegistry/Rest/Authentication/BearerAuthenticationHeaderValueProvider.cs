using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry
{
    class BearerToken 
    {   
        [JsonProperty("access_token")]
        public string AccessToken { get; set; }
        [JsonProperty("token_type")]
        public string TokenType { get; set; }
        [JsonProperty("expires_in")]
        public int ExpiresIn { get; set; }
        [JsonProperty("scope")]
        public string Scope { get; set; }
        [JsonIgnore]
        public double ExpiryTime { get; set; }
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
        private readonly HttpClient httpClient;
        private volatile BearerToken token;
        private const float tokenExpiryThreshold = 0.8f;

        public BearerAuthenticationHeaderValueProvider(
            HttpClient httpClient,
            string clientId, 
            string clientSecret, 
            string scope, 
            string tokenEndpoint, 
            string logicalCluster, 
            string identityPool, 
            int maxRetries, 
            int retriesWaitMs, 
            int retriesMaxWaitMs)
        {
            this.httpClient = httpClient;
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

        public async Task InitOrRefreshAsync()
        {
            await GenerateToken().ConfigureAwait(false);
        }

        public bool NeedsInitOrRefresh()
        {
            return token == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() >= token.ExpiryTime;
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
            for (int i = 0; i < maxRetries + 1; i++){
                try 
                {
                    var request = CreateTokenRequest();
                    var response = await httpClient.SendAsync(request).ConfigureAwait(continueOnCapturedContext: false);
                    response.EnsureSuccessStatusCode();
                    var tokenResponse = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    token = JObject.Parse(tokenResponse).ToObject<BearerToken>(JsonSerializer.Create());
                    token.ExpiryTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + (int)(token.ExpiresIn * tokenExpiryThreshold);
                    return;
                }
                catch (Exception e)
                {
                    if (i == maxRetries)
                    {
                        throw new Exception(
                            $"Failed to fetch token from server: {e.GetType().FullName} - {e.Message}");
                    }
                    await Task.Delay(RetryUtility.CalculateRetryDelay(retriesWaitMs, retriesMaxWaitMs, i))
                        .ConfigureAwait(false);
                }
            }
        }

        public AuthenticationHeaderValue GetAuthenticationHeader()
        {
            if (this.token == null)
            {
                throw new InvalidOperationException("Token not initialized");
            }

            return new AuthenticationHeaderValue("Bearer", this.token.AccessToken);
        }

        public string GetLogicalCluster() => this.logicalCluster;

        /// <summary>
        ///   Returns the identity pool ID(s). May be a single ID or comma-separated list.
        /// </summary>
        public string GetIdentityPool() => this.identityPool;

        public void Dispose()
        {
            this.token = null;
        }
    }
}