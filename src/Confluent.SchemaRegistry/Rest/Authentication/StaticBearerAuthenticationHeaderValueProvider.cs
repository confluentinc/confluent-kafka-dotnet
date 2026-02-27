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

    public class StaticBearerAuthenticationHeaderValueProvider : IAuthenticationBearerHeaderValueProvider, IDisposable
    {
        private readonly string token;
        private readonly string logicalCluster;
        private readonly string identityPool;

        public StaticBearerAuthenticationHeaderValueProvider(string token, string logicalCluster, string identityPool)
        {
            this.token = token;
            this.logicalCluster = logicalCluster;
            this.identityPool = identityPool;
        }

        public async Task InitOrRefreshAsync()
        {
            return;
        }

        public bool NeedsInitOrRefresh()
        {
            return false;

        }

        public AuthenticationHeaderValue GetAuthenticationHeader() => new AuthenticationHeaderValue("Bearer", token);

        public string GetLogicalCluster() => this.logicalCluster;

        public string GetIdentityPool() => this.identityPool;

        public void Dispose()
        {
            return;
        }
    }
}