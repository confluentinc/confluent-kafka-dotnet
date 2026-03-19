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

/// <summary>
///     An example showing Schema Registry authentication using
///     Azure Instance Metadata Service (IMDS) with a User Assigned
///     Managed Identity (UAMI).
///
///     Run this on an Azure VM with a UAMI attached:
///
///       dotnet run --project examples/SchemaRegistryOAuthAzureIMDS -- \
///         --schema.registry.url https://psrc-123456.us-east-1.aws.confluent.cloud \
///         --uami.client.id YOUR_UAMI_CLIENT_ID \
///         --uami.resource api://YOUR_BOOTSTRAP_ID \
///         --bearer.auth.logical.cluster lsrc-XXXXX \
///         --bearer.auth.identity.pool.id pool-XXXXX
/// </summary>
namespace Confluent.Kafka.Examples.SchemaRegistryOAuthAzureIMDS
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var parsed = ParseArgs(args);

            string schemaRegistryUrl = GetArg(parsed, "schema.registry.url",
                "https://psrc-123456.us-east-1.aws.confluent.cloud");
            string uamiClientId = GetArg(parsed, "uami.client.id",
                "YOUR_UAMI_CLIENT_ID");
            string resource = GetArg(parsed, "uami.resource",
                "api://YOUR_BOOTSTRAP_ID");
            string logicalCluster = GetArg(parsed, "bearer.auth.logical.cluster",
                "lsrc-XXXXX");
            string identityPoolId = GetArg(parsed, "bearer.auth.identity.pool.id",
                "pool-XXXXX");

            string endpointQuery = $"api-version=2025-04-07&resource={resource}&client_id={uamiClientId}";

            var config = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthBearerAzureIMDS,
                BearerAuthTokenEndpointQuery = endpointQuery,
                BearerAuthLogicalCluster = logicalCluster,
                BearerAuthIdentityPoolId = identityPoolId
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(config))
            {
                Console.WriteLine($"Fetching subjects from {schemaRegistryUrl} ...");
                var subjects = await schemaRegistry.GetAllSubjectsAsync();
                Console.WriteLine($"Subjects: [{string.Join(", ", subjects)}]");
            }
        }

        private static Dictionary<string, string> ParseArgs(string[] args)
        {
            var result = new Dictionary<string, string>();
            for (int i = 0; i < args.Length - 1; i += 2)
            {
                if (args[i].StartsWith("--"))
                {
                    result[args[i].Substring(2)] = args[i + 1];
                }
            }
            return result;
        }

        private static string GetArg(Dictionary<string, string> parsed, string key, string fallback)
        {
            return parsed.TryGetValue(key, out var value) ? value : fallback;
        }
    }
}
