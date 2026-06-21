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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.OAuthBearer.Aws.Internal
{
    /// <summary>
    ///     Parses the typed <c>sasl.oauthbearer.extensions</c> config key
    ///     (comma-separated <c>key=value</c> pairs) into a
    ///     dictionary suitable for the OAUTHBEARER refresh handoff.
    /// </summary>
    /// <remarks>
    ///     The grammar mirrors the cross-language convention for consistency.
    /// </remarks>
    internal static class AwsSaslExtensionsParser
    {
        /// <summary>
        ///     Config key carrying the SASL extensions list.
        /// </summary>
        private const string ConfigKey = "sasl.oauthbearer.extensions";

        /// <summary>
        ///     Parses the <c>sasl.oauthbearer.extensions</c> raw string into a
        ///     dictionary suitable for the OAUTHBEARER refresh handoff. Returns
        ///     <c>null</c> when <paramref name="raw"/> is null or empty so
        ///     downstream code can short-circuit.
        /// </summary>
        /// <exception cref="ArgumentException">
        ///     Malformed entry — missing <c>=</c>, or empty key (e.g.
        ///     <c>=value</c>).
        /// </exception>
        internal static IDictionary<string, string> Parse(string raw)
        {
            if (string.IsNullOrEmpty(raw)) return null;

            var result = new Dictionary<string, string>();
            foreach (var kv in KvStringParser.Parse(
                raw,
                new[] { ',' },
                contextLabel: ConfigKey))
            {
                // Last-wins on duplicate keys, mirroring librdkafka.
                result[kv.Key] = kv.Value;
            }
            return result.Count == 0 ? null : result;
        }
    }
}
