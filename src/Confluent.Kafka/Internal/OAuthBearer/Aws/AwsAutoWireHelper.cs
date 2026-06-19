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
    public static class AwsAutoWireHelper
    {
        private const string SaslOauthbearerMethodKey = "sasl.oauthbearer.method";
        private const string SaslOauthbearerMethodOidcValue = "oidc";
        private const string SaslOauthbearerMethodDefaultValue = "default";
        private const string SaslOauthbearerConfigKey = "sasl.oauthbearer.config";

        /// <summary>
        ///     Snapshots an enumerable config into a dictionary, applying
        ///     last-key-wins semantics to match librdkafka's behavior.
        /// </summary>
        public static IReadOnlyDictionary<string, string> SnapshotConfig(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            var dict = new Dictionary<string, string>();
            if (config == null) return dict;
            foreach (var kv in config)
            {
                dict[kv.Key] = kv.Value;
            }
            return dict;
        }

        /// <summary>
        ///     Returns <c>true</c> when the snapshot contains the AWS IAM marker
        ///     (<see cref="AwsIamMarker.Key"/> set to <see cref="AwsIamMarker.Value"/>).
        /// </summary>
        public static bool HasAwsIamMarker(IReadOnlyDictionary<string, string> snapshot)
            => snapshot.TryGetValue(AwsIamMarker.Key, out var value)
               && string.Equals(value, AwsIamMarker.Value, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        ///     Throws <see cref="InvalidOperationException"/> unless the snapshot
        ///     has <c>sasl.oauthbearer.method=oidc</c>. AWS IAM auth is declared
        ///     uniformly across all clients with <c>method=oidc</c> (the user's
        ///     intent: managed, OIDC-family auth); the binding then rewrites it to
        ///     <c>method=default</c> internally — see
        ///     <see cref="RewriteConfigIfAwsIamEnabled"/> — before the librdkafka handle
        ///     is created. <c>method=oidc</c> is therefore required as the explicit
        ///     opt-in, and any other value is rejected by design.
        /// </summary>
        public static void RequireMethodIsOidc(IReadOnlyDictionary<string, string> snapshot)
        {
            var hasMethod = snapshot.TryGetValue(SaslOauthbearerMethodKey, out var method);
            if (hasMethod && string.Equals(method, SaslOauthbearerMethodOidcValue, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            var actual = hasMethod ? $"'{method}'" : "<unset>";
            throw new InvalidOperationException(
                $"'{AwsIamMarker.Key}={AwsIamMarker.Value}' requires " +
                $"'{SaslOauthbearerMethodKey}=oidc' " +
                "(set SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc). " +
                $"Current value: {actual}. " +
                "method=oidc is mandatory for the AWS IAM authentication path.");
        }

        /// <summary>
        ///     Throws <see cref="InvalidOperationException"/> unless the snapshot
        ///     contains a non-empty <c>sasl.oauthbearer.config</c>. The AWS IAM
        ///     autowire path needs this config to carry the STS region, OIDC
        ///     audience, and any other parsed knobs.
        /// </summary>
        public static void RequireSaslOauthbearerConfig(IReadOnlyDictionary<string, string> snapshot)
        {
            if (snapshot.TryGetValue(SaslOauthbearerConfigKey, out var raw)
                && !string.IsNullOrEmpty(raw))
            {
                return;
            }

            throw new InvalidOperationException(
                $"'{AwsIamMarker.Key}={AwsIamMarker.Value}' is set but " +
                $"'{SaslOauthbearerConfigKey}' is missing or empty. The AWS IAM " +
                "autowire path requires region and audience to be supplied via " +
                "sasl.oauthbearer.config (e.g. \"region=us-east-1 audience=https://...\").");
        }

        /// <summary>
        ///     Whether to enable the AWS IAM autowire path for this config snapshot.
        /// </summary>
        /// <returns>
        ///     <c>true</c> if the marker is present and prerequisites are satisfied;
        ///     <c>false</c> if the marker is absent.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        ///     The marker is present, but <c>sasl.oauthbearer.method</c> is not
        ///     <c>oidc</c>, or <c>sasl.oauthbearer.config</c> is missing/empty.
        /// </exception>
        public static bool ShouldAutoWire(IReadOnlyDictionary<string, string> snapshot)
        {
            if (!HasAwsIamMarker(snapshot)) return false;
            RequireMethodIsOidc(snapshot);
            RequireSaslOauthbearerConfig(snapshot);
            return true;
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
        public static IEnumerable<KeyValuePair<string, string>> RewriteConfigIfAwsIamEnabled(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            if (config == null) return config;
            if (!HasAwsIamMarker(SnapshotConfig(config))) return config;

            var rewritten = new List<KeyValuePair<string, string>>();
            foreach (var kv in config)
            {
                if (string.Equals(kv.Key, AwsIamMarker.Key, StringComparison.Ordinal)) continue;
                if (string.Equals(kv.Key, SaslOauthbearerMethodKey, StringComparison.Ordinal)) continue;
                rewritten.Add(kv);
            }

            // Re-add method explicitly as default.
            rewritten.Add(new KeyValuePair<string, string>(
                SaslOauthbearerMethodKey, SaslOauthbearerMethodDefaultValue));
            return rewritten;
        }
    }
}
