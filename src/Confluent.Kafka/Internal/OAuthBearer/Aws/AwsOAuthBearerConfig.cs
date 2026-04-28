// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Typed view of the <c>sasl.oauthbearer.config</c> string used by the
    ///     reflection autowire path. Populated by <see cref="Parse"/>.
    /// </summary>
    /// <remarks>
    ///     Grammar (whitespace-separated <c>key=value</c> pairs, no quoting):
    ///     <code>
    ///       region=&lt;aws-region&gt;            (required)
    ///       audience=&lt;oidc-audience&gt;       (required)
    ///       duration_seconds=&lt;60..3600&gt;    (default: 300)
    ///       signing_algorithm=ES384|RS256       (default: ES384)
    ///       sts_endpoint=&lt;url&gt;             (optional, FIPS / VPC)
    ///       principal_name=&lt;value&gt;         (optional, override JWT 'sub')
    ///       extension_&lt;name&gt;=&lt;value&gt;  (zero or more SASL extensions)
    ///     </code>
    /// </remarks>
    public sealed class AwsOAuthBearerConfig
    {
        internal const string DefaultSigningAlgorithm = "ES384";
        internal const int DefaultDurationSeconds = 300;
        internal const int MinDurationSeconds = 60;
        internal const int MaxDurationSeconds = 3600;
        private const string ExtensionKeyPrefix = "extension_";

        private AwsOAuthBearerConfig(
            string region,
            string audience,
            string signingAlgorithm,
            TimeSpan duration,
            string stsEndpointOverride,
            string principalNameOverride,
            IDictionary<string, string> saslExtensions)
        {
            Region = region;
            Audience = audience;
            SigningAlgorithm = signingAlgorithm;
            Duration = duration;
            StsEndpointOverride = stsEndpointOverride;
            PrincipalNameOverride = principalNameOverride;
            SaslExtensions = saslExtensions;
        }

        /// <summary>AWS region targeted by the STS call (e.g. <c>us-east-1</c>).</summary>
        public string Region { get; }

        /// <summary>OIDC audience claim the relying party expects.</summary>
        public string Audience { get; }

        /// <summary>Signing algorithm (<c>ES384</c> or <c>RS256</c>); defaults to <c>ES384</c>.</summary>
        public string SigningAlgorithm { get; }

        /// <summary>Requested token lifetime; defaults to 300 seconds, bounded 60–3600.</summary>
        public TimeSpan Duration { get; }

        /// <summary>Optional STS endpoint URL (FIPS, VPC, etc.). <c>null</c> when unset.</summary>
        public string StsEndpointOverride { get; }

        /// <summary>Optional override for the OAUTHBEARER principal name. <c>null</c> means extract JWT <c>sub</c>.</summary>
        public string PrincipalNameOverride { get; }

        /// <summary>SASL extensions to send to the broker (RFC 7628 §3.1). <c>null</c> when none parsed.</summary>
        public IDictionary<string, string> SaslExtensions { get; }

        /// <summary>
        ///     Parses the value of the <c>sasl.oauthbearer.config</c> property
        ///     into a typed config, applying defaults and validating fields.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="raw"/> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     A required key is missing, an unknown key appears, or a value is
        ///     malformed / out of range.
        /// </exception>
        public static AwsOAuthBearerConfig Parse(string raw)
        {
            if (raw == null) throw new ArgumentNullException(nameof(raw));

            string region = null;
            string audience = null;
            string signingAlgorithm = null;
            string stsEndpoint = null;
            string principalName = null;
            int? durationSeconds = null;
            Dictionary<string, string> extensions = null;

            foreach (var token in raw.Split(new[] { ' ', '\t', '\r', '\n' },
                                            StringSplitOptions.RemoveEmptyEntries))
            {
                var idx = token.IndexOf('=');
                if (idx <= 0)
                {
                    throw new ArgumentException(
                        $"Malformed sasl.oauthbearer.config entry '{token}' (expected key=value).");
                }
                var key = token.Substring(0, idx);
                var value = token.Substring(idx + 1);

                switch (key)
                {
                    case "region":
                        AssertNotEmpty(key, value);
                        region = value;
                        break;
                    case "audience":
                        AssertNotEmpty(key, value);
                        audience = value;
                        break;
                    case "duration_seconds":
                        if (!int.TryParse(value, out var d))
                        {
                            throw new ArgumentException(
                                $"sasl.oauthbearer.config 'duration_seconds' must be an integer; got '{value}'.");
                        }
                        durationSeconds = d;
                        break;
                    case "signing_algorithm":
                        AssertNotEmpty(key, value);
                        signingAlgorithm = value;
                        break;
                    case "sts_endpoint":
                        AssertNotEmpty(key, value);
                        stsEndpoint = value;
                        break;
                    case "principal_name":
                        AssertNotEmpty(key, value);
                        principalName = value;
                        break;
                    default:
                        if (key.StartsWith(ExtensionKeyPrefix, StringComparison.Ordinal))
                        {
                            var name = key.Substring(ExtensionKeyPrefix.Length);
                            if (name.Length == 0)
                            {
                                throw new ArgumentException(
                                    $"sasl.oauthbearer.config extension key '{key}' has empty name.");
                            }
                            extensions ??= new Dictionary<string, string>();
                            extensions[name] = value;
                        }
                        else
                        {
                            throw new ArgumentException(
                                $"Unknown key '{key}' in sasl.oauthbearer.config.");
                        }
                        break;
                }
            }

            if (string.IsNullOrEmpty(region))
            {
                throw new ArgumentException("'region' is required in sasl.oauthbearer.config.");
            }
            if (string.IsNullOrEmpty(audience))
            {
                throw new ArgumentException("'audience' is required in sasl.oauthbearer.config.");
            }
            if (signingAlgorithm != null
                && signingAlgorithm != "ES384"
                && signingAlgorithm != "RS256")
            {
                throw new ArgumentException(
                    $"sasl.oauthbearer.config 'signing_algorithm' must be 'ES384' or 'RS256'; got '{signingAlgorithm}'.");
            }
            if (durationSeconds.HasValue
                && (durationSeconds.Value < MinDurationSeconds
                    || durationSeconds.Value > MaxDurationSeconds))
            {
                throw new ArgumentException(
                    $"sasl.oauthbearer.config 'duration_seconds' must be between " +
                    $"{MinDurationSeconds} and {MaxDurationSeconds} inclusive; got {durationSeconds.Value}.");
            }

            return new AwsOAuthBearerConfig(
                region: region,
                audience: audience,
                signingAlgorithm: signingAlgorithm ?? DefaultSigningAlgorithm,
                duration: TimeSpan.FromSeconds(durationSeconds ?? DefaultDurationSeconds),
                stsEndpointOverride: stsEndpoint,
                principalNameOverride: principalName,
                saslExtensions: extensions);
        }

        private static void AssertNotEmpty(string key, string value)
        {
            if (value.Length == 0)
            {
                throw new ArgumentException(
                    $"sasl.oauthbearer.config '{key}' must not be empty.");
            }
        }
    }
}
