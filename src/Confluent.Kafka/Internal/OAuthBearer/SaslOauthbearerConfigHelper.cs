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

using System;
using System.Collections.Generic;
using Confluent.Kafka.Internal.OAuthBearer.Aws;

namespace Confluent.Kafka.Internal.OAuthBearer
{
    /// <summary>
    ///     Common OAUTHBEARER helpers shared by the Producer/Consumer/AdminClient
    ///     builders.
    /// </summary>
    internal static class SaslOauthbearerConfigHelper
    {
        internal const string SaslOauthbearerMethodKey = "sasl.oauthbearer.method";
        internal const string SaslOauthbearerMethodOidcValue = "oidc";
        internal const string SaslOauthbearerMethodDefaultValue = "default";
        internal const string SaslOauthbearerConfigKey = "sasl.oauthbearer.config";

        /// <summary>
        ///     Throws <see cref="ArgumentException"/> unless the snapshot has
        ///     <c>sasl.oauthbearer.method=oidc</c>. Callers declare OAUTHBEARER
        ///     auth uniformly with <c>method=oidc</c>; any other value is rejected.
        /// </summary>
        internal static void RequireMethodIsOidc(IReadOnlyDictionary<string, string> snapshot)
        {
            var hasMethod = snapshot.TryGetValue(SaslOauthbearerMethodKey, out var method);
            if (hasMethod && string.Equals(method, SaslOauthbearerMethodOidcValue, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            var actual = hasMethod ? $"'{method}'" : "<unset>";
            throw new ArgumentException(
                "SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc is required. " +
                $"Current value: {actual}.");
        }

        /// <summary>
        ///     Throws <see cref="ArgumentException"/> unless the snapshot contains a
        ///     non-empty <c>sasl.oauthbearer.config</c>.
        /// </summary>
        internal static void RequireSaslOauthbearerConfig(IReadOnlyDictionary<string, string> snapshot)
        {
            if (snapshot.TryGetValue(SaslOauthbearerConfigKey, out var raw)
                && !string.IsNullOrEmpty(raw))
            {
                return;
            }

            throw new ArgumentException(
                "SaslOauthbearerConfig is missing or empty.");
        }

        /// <summary>
        ///     Selects the OAUTHBEARER refresh handler for a builder, with the
        ///     precedence: explicit handler &gt; AWS IAM marker autowire &gt; none.
        ///     Shared by the Producer and Consumer builders via <see cref="IClient"/>;
        ///     each builder binds its typed handler to a plain <c>Action&lt;string&gt;</c>
        ///     before delegating.
        /// </summary>
        /// <param name="client">
        ///     The producer/consumer the autowired handler binds to when the AWS IAM
        ///     marker is present.
        /// </param>
        /// <param name="explicitHandler">
        ///     The user's explicit refresh handler, already bound to
        ///     <paramref name="client"/>, or <c>null</c> if none was set.
        /// </param>
        /// <param name="snapshot">The client config snapshot (last-key-wins).</param>
        internal static Action<string> ResolveOAuthBearerHandler(
            IClient client,
            Action<string> explicitHandler,
            IReadOnlyDictionary<string, string> snapshot)
        {
            if (explicitHandler != null) return explicitHandler;

            if (AwsAutoWireHelper.ShouldAutoWire(snapshot))
            {
                AwsAutoWireHelper.Validate(snapshot);
                var handler = AwsAutoWireDispatcher.LoadHandler(snapshot);
                return oAuthBearerConfig => handler(client, oAuthBearerConfig);
            }

            return null;
        }
    }
}
