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
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;


namespace Confluent.SchemaRegistry
{
    class BearerAuthenticationHeaderValueProviderBuilder : AbstractAuthenticationHeaderValueProviderBuilder
    {
        private string clientId;
        private string clientSecret;
        private string tokenEndpointUrl;
        private string scope;

        internal BearerAuthenticationHeaderValueProviderBuilder(
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
            IEnumerable<KeyValuePair<string, string>> config) : base(authenticationHeaderValueProvider, config)
        {
        }

        protected override void Validate()
        {
            base.Validate();

            clientId = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientId).Value;

            clientSecret = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthClientSecret).Value;

            scope = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthScope).Value;

            tokenEndpointUrl = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthTokenEndpointUrl).Value;

            if (tokenEndpointUrl == null || clientId == null || clientSecret == null || scope == null)
            {
                throw new ArgumentException(
                    $"Invalid bearer authentication provider configuration: Token endpoint URL, client ID, client secret, and scope must be specified");
            }
        }

        public override IAuthenticationBearerHeaderValueProvider Build(
            int maxRetries, int retriesWaitMs, int retriesMaxWaitMs)
        { 
            Validate();
            return new BearerAuthenticationHeaderValueProvider(
                new HttpClient(),
                clientId, clientSecret, scope,
                tokenEndpointUrl,
                logicalCluster, identityPoolId,
                maxRetries, retriesWaitMs, retriesMaxWaitMs);
        }
    }
}