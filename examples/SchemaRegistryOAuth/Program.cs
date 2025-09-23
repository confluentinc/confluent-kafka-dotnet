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

using Confluent.SchemaRegistry;
using System.Net.Http.Headers;
/// <summary>
///     An example showing schema registry authentication 
///     with an OAUTHBEARER token implementation.
/// </summary>
namespace Confluent.Kafka.Examples.SchemaRegistryOAuth
{

    public class Program
    {   

        private class ExampleBearerAuthProvider : IAuthenticationBearerHeaderValueProvider
        {
            private string token;
            private string logicalCluster;
            private string identityPool;


            public ExampleBearerAuthProvider(string token, string logicalCluster, string identityPool)
            {
                this.token = token;
                this.logicalCluster = logicalCluster;
                this.identityPool = identityPool;
            }

            public string GetBearerToken() => token;
            public AuthenticationHeaderValue GetAuthenticationHeader() => new AuthenticationHeaderValue("Bearer", token);
            public string GetLogicalCluster() => logicalCluster;
            public string GetIdentityPool() => identityPool;
            public bool NeedsInitOrRefresh() => false;
            public Task InitOrRefreshAsync() => Task.CompletedTask;
        }

        public static async Task Main(string[] args)
        {
            if (args.Length >= 9)
            {
                Console.WriteLine("Usage: .. schemaRegistryUrl clientId clientSecret scope tokenEndpoint logicalCluster identityPool token [azureIMDSQueryParams]");
                return;
            }
            string schemaRegistryUrl = args[1];
            string clientId = args[2];
            string clientSecret = args[3];
            string scope = args[4];
            string tokenEndpoint = args[5];
            string logicalCluster = args[6];
            string identityPool = args[7];
            string token = args[8];
            string azureIMDSQueryParams = null;
            if (args.Length >= 10)
            { 
                azureIMDSQueryParams = args[9];
            }

            //using BearerAuthCredentialsSource.OAuthBearer
                var clientCredentialsSchemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthBearer,
                BearerAuthClientId = clientId,
                BearerAuthClientSecret = clientSecret,
                BearerAuthScope = scope,
                BearerAuthTokenEndpointUrl = tokenEndpoint,
                BearerAuthLogicalCluster = logicalCluster,
                BearerAuthIdentityPoolId = identityPool
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(clientCredentialsSchemaRegistryConfig))
            {
                var subjects = await schemaRegistry.GetAllSubjectsAsync();
                Console.WriteLine(string.Join(", ", subjects));
            }

            //using BearerAuthCredentialsSource.StaticToken
            var staticSchemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.StaticToken,
                BearerAuthToken = token,
                BearerAuthLogicalCluster = logicalCluster,
                BearerAuthIdentityPoolId = identityPool
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(staticSchemaRegistryConfig))
            {
                var subjects = await schemaRegistry.GetAllSubjectsAsync();
                Console.WriteLine(string.Join(", ", subjects));
            }

            //Using BearerAuthCredentialsSource.Custom
            var customSchemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.Custom
            };

            var customProvider = new ExampleBearerAuthProvider(token, logicalCluster, identityPool);
            using (var schemaRegistry = new CachedSchemaRegistryClient(customSchemaRegistryConfig, customProvider))
            {
                var subjects = await schemaRegistry.GetAllSubjectsAsync();
                Console.WriteLine(string.Join(", ", subjects));
            }
            
            if (azureIMDSQueryParams == null)
            {
                return;
            }

            //Using BearerAuthCredentialsSource.OAuthOIDCAzureIMDS
            var azureIMDSSchemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthOIDCAzureIMDS,
                SchemaRegistryBearerAuthTokenEndpointQuery = azureIMDSQueryParams,
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(azureIMDSSchemaRegistryConfig))
            {
                var subjects = await schemaRegistry.GetAllSubjectsAsync();
                Console.WriteLine(string.Join(", ", subjects));
            }
        }
    }
}
