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
    /// <summary>
    /// Provides authentication header values using a static bearer token.
    /// </summary>
    public class StaticBearerAuthenticationHeaderValueProvider : IAuthenticationBearerHeaderValueProvider, IDisposable
    {
        private readonly string token;
        private readonly string logicalCluster;
        private readonly string identityPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="StaticBearerAuthenticationHeaderValueProvider"/> class.
        /// </summary>
        /// <param name="token">The bearer token to use for authentication.</param>
        /// <param name="logicalCluster">The logical cluster identifier.</param>
        /// <param name="identityPool">The identity pool identifier.</param>
        public StaticBearerAuthenticationHeaderValueProvider(string token, string logicalCluster, string identityPool)
        {
            this.token = token;
            this.logicalCluster = logicalCluster;
            this.identityPool = identityPool;
        }

        /// <inheritdoc/>
        public Task InitOrRefreshAsync()
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public bool NeedsInitOrRefresh()
        {
            return false;

        }

        /// <inheritdoc/>
        public AuthenticationHeaderValue GetAuthenticationHeader() => new AuthenticationHeaderValue("Bearer", token);

        /// <inheritdoc/>
        public string GetLogicalCluster() => this.logicalCluster;

        /// <inheritdoc/>
        public string GetIdentityPool() => this.identityPool;

        /// <inheritdoc/>
        public void Dispose()
        {
            return;
        }
    }
}