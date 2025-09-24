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


    abstract class AbstractAuthenticationHeaderValueProviderBuilder : IAuthenticationBearerHeaderValueProviderBuilder
    {
        protected string logicalCluster;

        protected string identityPoolId;

        protected IEnumerable<KeyValuePair<string, string>> config;

        private IAuthenticationHeaderValueProvider authenticationHeaderValueProvider;

        protected AbstractAuthenticationHeaderValueProviderBuilder(
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider,
            IEnumerable<KeyValuePair<string, string>> config)
        {
            this.authenticationHeaderValueProvider = authenticationHeaderValueProvider;
            this.config = config;
        }

        protected virtual void Validate()
        {
            if (authenticationHeaderValueProvider != null)
            {
                throw new ArgumentException(
                    $"Invalid authentication header value provider configuration: Cannot specify both custom provider and bearer authentication");
            }
            logicalCluster = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthLogicalCluster).Value;

            identityPoolId = config.FirstOrDefault(prop =>
                prop.Key.ToLower() == SchemaRegistryConfig.PropertyNames.SchemaRegistryBearerAuthIdentityPoolId).Value;
            if (logicalCluster == null || identityPoolId == null)
            {
                throw new ArgumentException(
                    $"Invalid bearer authentication provider configuration: Logical cluster and identity pool ID must be specified");
            }
        }

        public abstract IAuthenticationBearerHeaderValueProvider Build(
            int maxRetries, int retriesWaitMs, int retriesMaxWaitMs);
    }
}
