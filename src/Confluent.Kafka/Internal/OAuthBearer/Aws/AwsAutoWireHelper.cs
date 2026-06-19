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

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Shared helpers used by Producer/Consumer/AdminClient builders to
    ///     dispatch the AWS IAM autowire path.
    /// </summary>
    internal static class AwsAutoWireHelper
    {
        /// <summary>
        ///     returns <c>true</c> when the snapshot contains the AWS IAM marker
        ///     (<see cref="AwsIamMarker.Key"/> set to <see cref="AwsIamMarker.Value"/>),
        ///     <c>false</c> otherwise. The caller is responsible for calling
        ///     <see cref="Validate"/>.
        /// </summary>
        internal static bool ShouldAutoWire(IReadOnlyDictionary<string, string> snapshot)
            => snapshot.TryGetValue(AwsIamMarker.Key, out var value)
               && string.Equals(value, AwsIamMarker.Value, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        ///     Validates the OAUTHBEARER prerequisites for the AWS IAM autowire path
        ///     (<c>method=oidc</c> and a non-empty <c>sasl.oauthbearer.config</c>).
        ///     Called only when the marker is present.
        /// </summary>
        /// <exception cref="ArgumentException">
        ///     <c>sasl.oauthbearer.method</c> is not <c>oidc</c>, or
        ///     <c>sasl.oauthbearer.config</c> is missing/empty.
        /// </exception>
        internal static void Validate(IReadOnlyDictionary<string, string> snapshot)
        {
            SaslOauthbearerConfigHelper.RequireMethodIsOidc(snapshot);
            SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig(snapshot);
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

            if (ShouldAutoWire(snapshot))
            {
                Validate(snapshot);
                var handler = AwsAutoWireDispatcher.LoadHandler(snapshot);
                return oAuthBearerConfig => handler(client, oAuthBearerConfig);
            }

            return null;
        }
    }
}
