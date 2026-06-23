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
    internal readonly struct OAuthBearerToken
    {
        /// <summary>
        ///     The JWT itself — passed verbatim to the broker as the OAUTHBEARER
        ///     <c>Bearer</c> value.
        /// </summary>
        internal string TokenValue { get; }

        /// <summary>
        ///     Expiry expressed as Unix milliseconds since epoch. librdkafka uses
        ///     this to schedule the next refresh.
        /// </summary>
        internal long LifetimeMs { get; }

        /// <summary>
        ///     Kafka principal name — the JWT's <c>sub</c> claim (the bare
        ///     role ARN for AWS-minted tokens).
        /// </summary>
        internal string PrincipalName { get; }

        /// <summary>
        ///     Optional SASL extensions forwarded to the broker. <c>null</c> when
        ///     no extensions are configured.
        /// </summary>
        internal IDictionary<string, string> Extensions { get; }

        internal OAuthBearerToken(
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
