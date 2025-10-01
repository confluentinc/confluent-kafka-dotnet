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
using System.Collections.Generic;
using System.Linq;


namespace Confluent.SchemaRegistry
{
    class StaticAuthenticationHeaderValueProviderBuilder :
        AbstractAuthenticationHeaderValueProviderBuilder
    {
        private string bearerToken;

        internal StaticAuthenticationHeaderValueProviderBuilder(
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
            IEnumerable<KeyValuePair<string, string>> config) :
            base(authenticationHeaderValueProvider, config)
        {
        }

        protected override void Validate()
        {
            base.Validate();
            bearerToken = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthToken).Value;

            if (bearerToken == null)
            {
                throw new ArgumentException(
                    $"Invalid authentication header value provider configuration: Bearer authentication token not specified");
            }
        }

        public override IAuthenticationBearerHeaderValueProvider Build(
            int maxRetries, int retriesWaitMs, int retriesMaxWaitMs)
        {
            Validate();
            return new StaticBearerAuthenticationHeaderValueProvider(
                bearerToken, logicalCluster, identityPoolId);
        }
    }
}
