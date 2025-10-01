using System;
using System.Net.Http;
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

    /// <summary>
    /// Provides bearer token authentication header values for schema registry requests
    /// based on OAuth2 client credentials flow.
    /// </summary>
    public class BearerAuthenticationHeaderValueProvider : AbstractBearerAuthenticationHeaderValueProvider
    {
        private readonly string clientId;
        private readonly string clientSecret;
        private readonly string scope;
        private readonly string tokenEndpoint;
        private readonly HttpClient httpClient;
        private volatile BearerToken tokenObject;
        private const float tokenExpiryThreshold = 0.8f;

        /// <summary>
        /// Initializes a new instance of the <see cref="BearerAuthenticationHeaderValueProvider"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client used for requests.</param>
        /// <param name="clientId">The OAuth client ID.</param>
        /// <param name="clientSecret">The OAuth client secret.</param>
        /// <param name="scope">The OAuth scope.</param>
        /// <param name="tokenEndpoint">The OAuth token endpoint URL.</param>
        /// <param name="logicalCluster">The logical cluster name.</param>
        /// <param name="identityPool">The identity pool.</param>
        /// <param name="maxRetries">The maximum number of retries.</param>
        /// <param name="retriesWaitMs">The initial wait time between retries in milliseconds.</param>
        /// <param name="retriesMaxWaitMs">The maximum wait time between retries in milliseconds.</param>
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
            : base(logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs)
        {
            this.httpClient = httpClient;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.scope = scope;
            this.tokenEndpoint = tokenEndpoint;
        }

        /// <inheritdoc/>
        public override bool NeedsInitOrRefresh()
        {
            return tokenObject == null || DateTimeOffset.UtcNow.ToUnixTimeSeconds() >= tokenObject.ExpiryTime;
        }

        /// <inheritdoc/>
        protected override async Task<string> FetchToken(HttpRequestMessage request)
        {
            var response = await httpClient.SendAsync(request).ConfigureAwait(continueOnCapturedContext: false);
            response.EnsureSuccessStatusCode();
            var tokenResponse = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            tokenObject = JObject.Parse(tokenResponse).ToObject<BearerToken>(JsonSerializer.Create());
            tokenObject.ExpiryTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + (int)(tokenObject.ExpiresIn * tokenExpiryThreshold);
            return tokenObject.AccessToken;
        }

        /// <inheritdoc/>
        protected override HttpRequestMessage CreateTokenRequest()
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
        
        /// <inheritdoc/>
        protected override void DisposeSecrets()
        {
            base.DisposeSecrets();
            tokenObject = null;
        }
    }
}