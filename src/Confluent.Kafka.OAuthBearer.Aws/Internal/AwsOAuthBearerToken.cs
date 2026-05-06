// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;

namespace Confluent.Kafka.OAuthBearer.Aws.Internal
{
    /// <summary>
    ///     A minted OAUTHBEARER token ready to be handed to
    ///     <c>IClient.OAuthBearerSetToken</c>.
    /// </summary>
    internal readonly struct AwsOAuthBearerToken
    {
        /// <summary>
        ///     The JWT itself — passed verbatim to the broker as the OAUTHBEARER
        ///     <c>Bearer</c> value.
        /// </summary>
        public string TokenValue { get; }

        /// <summary>
        ///     Expiry expressed as Unix milliseconds since epoch. librdkafka uses
        ///     this to schedule the next refresh.
        /// </summary>
        public long LifetimeMs { get; }

        /// <summary>
        ///     Kafka principal name. Either the JWT's <c>sub</c> claim (bare
        ///     role ARN for AWS-minted tokens) or the user-supplied
        ///     <c>principal_name</c> override from the parsed config.
        /// </summary>
        public string PrincipalName { get; }

        /// <summary>
        ///     Optional SASL extensions forwarded to the broker. <c>null</c> when
        ///     no extensions are configured.
        /// </summary>
        public IDictionary<string, string> Extensions { get; }

        /// <summary>Constructs a token record. All fields are set here and immutable afterwards.</summary>
        public AwsOAuthBearerToken(
            string tokenValue,
            long lifetimeMs,
            string principalName,
            IDictionary<string, string> extensions)
        {
            TokenValue = tokenValue;
            LifetimeMs = lifetimeMs;
            PrincipalName = principalName;
            Extensions = extensions;
        }
    }
}
