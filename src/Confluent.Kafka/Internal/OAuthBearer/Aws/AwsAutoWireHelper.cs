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
        ///     Returns <c>true</c> when the snapshot contains the AWS IAM marker
        ///     (<see cref="AwsIamMarker.Key"/> set to <see cref="AwsIamMarker.Value"/>).
        /// </summary>
        internal static bool HasAwsIamMarker(IReadOnlyDictionary<string, string> snapshot)
            => snapshot.TryGetValue(AwsIamMarker.Key, out var value)
               && string.Equals(value, AwsIamMarker.Value, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        ///     Whether to enable the AWS IAM autowire path for this config snapshot.
        /// </summary>
        /// <returns>
        ///     <c>true</c> if the marker is present and prerequisites are satisfied;
        ///     <c>false</c> if the marker is absent.
        /// </returns>
        /// <exception cref="ArgumentException">
        ///     The marker is present, but <c>sasl.oauthbearer.method</c> is not
        ///     <c>oidc</c>, or <c>sasl.oauthbearer.config</c> is missing/empty.
        /// </exception>
        internal static bool ShouldAutoWire(IReadOnlyDictionary<string, string> snapshot)
        {
            if (HasAwsIamMarker(snapshot))
            {
                Validate(snapshot);
                return true;
            }

            return false;
        }

        /// <summary>
        ///     Validates the OAUTHBEARER prerequisites for the AWS IAM autowire path
        ///     (<c>method=oidc</c> and a non-empty <c>sasl.oauthbearer.config</c>).
        ///     Called only when the marker is present.
        /// </summary>
        /// <exception cref="ArgumentException">
        ///     <c>sasl.oauthbearer.method</c> is not <c>oidc</c>, or
        ///     <c>sasl.oauthbearer.config</c> is missing/empty.
        /// </exception>
        private static void Validate(IReadOnlyDictionary<string, string> snapshot)
        {
            SaslOauthbearerConfigHelper.RequireMethodIsOidc(snapshot);
            SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig(snapshot);
        }

        /// <summary>
        ///     Produces the config that is actually handed to librdkafka for the
        ///     AWS IAM autowire path. When the marker is present, the marker key is
        ///     stripped and <c>sasl.oauthbearer.method</c> is normalized to
        ///     <c>default</c>, so librdkafka runs the canonical OAUTHBEARER pattern
        ///     (<c>method=default</c> + an app-supplied token-refresh callback) and
        ///     never sees <c>aws_iam</c> — keeping the binding off librdkafka's
        ///     native aws_iam path and removing any dependency on an
        ///     AWS-IAM-aware librdkafka.
        /// </summary>
        /// <remarks>
        ///     <para>
        ///         When the marker is absent, the input enumerable is returned
        ///         unchanged (same reference, no allocation) so non-AWS configs see
        ///         zero behavior change.
        ///     </para>
        ///     <para>
        ///         This is a pure transform over a copy: it does not mutate the
        ///         caller's config, and it never throws. Marker / method / config
        ///         validation stays in <see cref="ShouldAutoWire"/>. The gate here
        ///         is marker-presence alone (not <see cref="ShouldAutoWire"/>) so the
        ///         marker is stripped even when an explicit token-refresh handler
        ///         takes precedence over autowire.
        ///     </para>
        /// </remarks>
        internal static IEnumerable<KeyValuePair<string, string>> RewriteConfigIfAwsIamEnabled(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            if (config == null) return config;
            if (!HasAwsIamMarker(Config.Snapshot(config))) return config;

            var rewritten = new List<KeyValuePair<string, string>>();
            foreach (var kv in config)
            {
                if (string.Equals(kv.Key, AwsIamMarker.Key, StringComparison.Ordinal)) continue;
                if (string.Equals(kv.Key, SaslOauthbearerConfigHelper.SaslOauthbearerMethodKey, StringComparison.Ordinal)) continue;
                rewritten.Add(kv);
            }

            // Re-add method explicitly as default.
            rewritten.Add(new KeyValuePair<string, string>(
                SaslOauthbearerConfigHelper.SaslOauthbearerMethodKey,
                SaslOauthbearerConfigHelper.SaslOauthbearerMethodDefaultValue));
            return rewritten;
        }
    }
}
