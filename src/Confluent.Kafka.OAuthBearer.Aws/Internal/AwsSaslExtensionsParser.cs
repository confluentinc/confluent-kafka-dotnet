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

namespace Confluent.Kafka.OAuthBearer.Aws.Internal
{
    /// <summary>
    ///     Parses the typed <c>sasl.oauthbearer.extensions</c> config key
    ///     (comma-separated <c>key=value</c> pairs, RFC 7628 §3.1) into a
    ///     dictionary suitable for the OAUTHBEARER refresh handoff.
    /// </summary>
    /// <remarks>
    ///     The grammar mirrors the cross-language convention used by Python,
    ///     Go, and JavaScript Kafka bindings — keep this parser consistent
    ///     with their behaviour if it ever evolves.
    /// </remarks>
    internal static class AwsSaslExtensionsParser
    {
        /// <summary>
        ///     Config key carrying the SASL extensions list. Matches the
        ///     <c>SaslOauthbearerExtensions</c> typed property surfaced to
        ///     consumer code by Confluent.Kafka core.
        /// </summary>
        public const string ConfigKey = "sasl.oauthbearer.extensions";

        /// <summary>
        ///     Parses the typed <c>sasl.oauthbearer.extensions</c> entry from
        ///     <paramref name="kafkaConfig"/>. Returns <c>null</c> when the
        ///     key is absent or empty so downstream code can short-circuit.
        /// </summary>
        /// <exception cref="ArgumentException">
        ///     Malformed entry — missing <c>=</c>, or empty key (e.g.
        ///     <c>=value</c>).
        /// </exception>
        public static IDictionary<string, string> Parse(
            IReadOnlyDictionary<string, string> kafkaConfig)
        {
            if (!kafkaConfig.TryGetValue(ConfigKey, out var raw)
                || string.IsNullOrEmpty(raw))
            {
                return null;
            }

            var result = new Dictionary<string, string>();
            foreach (var token in raw.Split(','))
            {
                var trimmed = token.Trim();
                if (trimmed.Length == 0) continue;

                var idx = trimmed.IndexOf('=');
                if (idx <= 0)
                {
                    throw new ArgumentException(
                        $"Malformed '{ConfigKey}' entry '{trimmed}' " +
                        "(expected comma-separated key=value pairs).");
                }
                var name = trimmed.Substring(0, idx);
                var value = trimmed.Substring(idx + 1);
                result[name] = value;
            }
            return result.Count == 0 ? null : result;
        }
    }
}
