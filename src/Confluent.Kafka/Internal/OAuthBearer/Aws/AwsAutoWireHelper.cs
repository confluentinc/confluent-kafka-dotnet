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
    ///     dispatch the AWS IAM autowire path. Centralised so the three builder
    ///     classes hold identical dispatch logic.
    /// </summary>
    public static class AwsAutoWireHelper
    {
        private const string SaslOauthbearerMethodKey = "sasl.oauthbearer.method";
        private const string SaslOauthbearerMethodOidcValue = "oidc";

        /// <summary>
        ///     Snapshots an enumerable config into a dictionary, applying
        ///     last-key-wins semantics (matches librdkafka's behaviour on
        ///     duplicate keys in the on-wire config).
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
        ///     has <c>sasl.oauthbearer.method=oidc</c>. The AWS IAM autowire path
        ///     runs as a high-level-client refresh callback inside librdkafka's
        ///     OIDC subsystem (parallel to Azure IMDS); without
        ///     <c>method=oidc</c> the configuration is rejected by design.
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
                "The AWS IAM path runs as a high-level-client refresh callback " +
                "inside librdkafka's OIDC subsystem; without method=oidc the " +
                "configuration is rejected by design.");
        }
    }
}
