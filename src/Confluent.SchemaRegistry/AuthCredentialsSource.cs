// Copyright 2020 Confluent Inc.
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


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Auth credentials source.
    /// </summary>
    public enum AuthCredentialsSource
    {
        /// <summary>
        ///     Credentials are specified via the `schema.registry.basic.auth.user.info` config property in the form username:password.
        ///     If `schema.registry.basic.auth.user.info` is not set, authentication is disabled.
        /// </summary>
        UserInfo,

        /// <summary>
        ///     Credentials are specified via the `sasl.username` and `sasl.password` configuration properties.
        /// </summary>
        SaslInherit
    }

    /// <summary>
    ///     Bearer auth credentials source.
    /// </summary>
    public enum BearerAuthCredentialsSource
    {
        /// <summary>
        ///     Credentials are specified via the `schema.registry.bearer.auth.token` config property.
        /// </summary>
        StaticToken,

        /// <summary>
        ///     Credentials are specified via the `schema.registry.bearer.auth.client.id` and
        ///     `schema.registry.bearer.auth.client.secret` config properties.
        /// </summary>
        OAuthBearer,

        /// <summary>
        ///     Metadata based authentication using Azure Instance Metadata Service (IMDS).
        ///     Only the token endpoint URL and/or query parameters need to be specified.
        /// </summary>
        OAuthBearerAzureIMDS,

        /// <summary>
        ///     User provides a custom implementation of IAuthenticationHeaderValueProvider.
        /// </summary>
        Custom
    }
}