// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry
{
    class AzureIMDSBearerToken 
    {   
        [JsonProperty("access_token")]
        public string AccessToken { get; set; }
        [JsonProperty("expires_in")]
        public int ExpiresIn { get; set; }
        [JsonIgnore]
        public double ExpiryTime { get; set; }
    }

    /// <summary>
    /// Provider for authentication header values that uses Azure Instance Metadata Service (IMDS) to obtain bearer tokens.
    /// </summary>
    public class AzureIMDSBearerAuthenticationHeaderValueProvider : AbstractBearerAuthenticationHeaderValueProvider
    {
        private readonly string tokenEndpoint;
        private readonly HttpClient httpClient;
        private volatile AzureIMDSBearerToken tokenObject;
        private const float tokenExpiryThreshold = 0.8f;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureIMDSBearerAuthenticationHeaderValueProvider"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client used to make requests.</param>
        /// <param name="tokenEndpoint">The endpoint URL to request tokens from.</param>
        /// <param name="logicalCluster">The logical cluster name.</param>
        /// <param name="identityPool">The identity pool identifier.</param>
        /// <param name="maxRetries">The maximum number of retry attempts.</param>
        /// <param name="retriesWaitMs">The initial wait time between retries in milliseconds.</param>
        /// <param name="retriesMaxWaitMs">The maximum wait time between retries in milliseconds.</param>
        public AzureIMDSBearerAuthenticationHeaderValueProvider(
            HttpClient httpClient,
            string tokenEndpoint, 
            string logicalCluster, 
            string identityPool, 
            int maxRetries, 
            int retriesWaitMs, 
            int retriesMaxWaitMs)
            : base(logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs)
        {
            this.httpClient = httpClient;
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
            tokenObject = JObject.Parse(tokenResponse).ToObject<AzureIMDSBearerToken>(JsonSerializer.Create());
            tokenObject.ExpiryTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + (int)(tokenObject.ExpiresIn * tokenExpiryThreshold);
            return tokenObject.AccessToken;
        }

        /// <inheritdoc/>
        protected override HttpRequestMessage CreateTokenRequest()
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, tokenEndpoint);

            request.Headers.Add("Metadata", "true");

            return request;
        }
    }
}