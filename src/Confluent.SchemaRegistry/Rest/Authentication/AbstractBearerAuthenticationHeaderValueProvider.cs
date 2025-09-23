using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    /// Abstract base class for providers that supply Bearer authentication header values.
    /// </summary>
    public abstract class AbstractBearerAuthenticationHeaderValueProvider : IAuthenticationBearerHeaderValueProvider, IDisposable
    {
        private readonly string logicalCluster;
        private readonly string identityPool;
        private readonly int maxRetries;
        private readonly int retriesWaitMs;
        private readonly int retriesMaxWaitMs;
        private string token;

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractBearerAuthenticationHeaderValueProvider"/> class.
        /// </summary>
        /// <param name="logicalCluster">The logical cluster name.</param>
        /// <param name="identityPool">The identity pool.</param>
        /// <param name="maxRetries">The maximum number of retries for token generation.</param>
        /// <param name="retriesWaitMs">The base wait time in milliseconds between retries.</param>
        /// <param name="retriesMaxWaitMs">The maximum wait time in milliseconds between retries.</param>
        protected AbstractBearerAuthenticationHeaderValueProvider(
            string logicalCluster,
            string identityPool,
            int maxRetries,
            int retriesWaitMs,
            int retriesMaxWaitMs)
        {
            this.logicalCluster = logicalCluster;
            this.identityPool = identityPool;
            this.maxRetries = maxRetries;
            this.retriesWaitMs = retriesWaitMs;
            this.retriesMaxWaitMs = retriesMaxWaitMs;
        }

        /// <inheritdoc/>
        public async Task InitOrRefreshAsync()
        {
            await GenerateToken().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public abstract bool NeedsInitOrRefresh();

        /// <summary>
        /// Fetches a token using the provided reusable HTTP request message.
        /// Can also generate a new request if needed.
        /// </summary>
        /// <param name="request">The HTTP request message to use for fetching the token.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the fetched token as a string.</returns>
        protected abstract Task<string> FetchToken(HttpRequestMessage request);

        /// <summary>
        /// Creates an HTTP request message for token acquisition.
        /// </summary>
        /// <returns>An HTTP request message configured for token acquisition.</returns>
        protected abstract HttpRequestMessage CreateTokenRequest();

        private async Task GenerateToken()
        {
            var request = CreateTokenRequest();

            for (int i = 0; i < maxRetries + 1; i++)
            {
                try
                {
                    token = await FetchToken(request);
                    return;
                }
                catch (Exception e)
                {
                    if (i == maxRetries)
                    {
                        throw new Exception("Failed to fetch token from server: " + e.Message);
                    }
                    await Task.Delay(RetryUtility.CalculateRetryDelay(retriesWaitMs, retriesMaxWaitMs, i))
                        .ConfigureAwait(false);
                }
            }
        }

        /// <inheritdoc/>
        public AuthenticationHeaderValue GetAuthenticationHeader()
        {
            if (this.token == null)
            {
                throw new InvalidOperationException("Token not initialized");
            }

            return new AuthenticationHeaderValue("Bearer", this.token);
        }

        /// <inheritdoc/>
        public string GetLogicalCluster() => this.logicalCluster;

        /// <inheritdoc/>
        public string GetIdentityPool() => this.identityPool;

        /// <summary>
        /// Disposes of any sensitive information held by this provider.
        /// </summary>
        protected virtual void DisposeSecrets()
        {
             token = null;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            DisposeSecrets();
        }
    }
}