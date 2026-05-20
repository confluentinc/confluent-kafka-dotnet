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

namespace Confluent.Kafka.Internal
{
    /// <summary>
    ///     Shared utility for parsing <c>key=value</c> strings used by several
    ///     SASL-related configs.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The default trimming behaviour mirrors librdkafka's logic for consistency.
    ///     </para>
    /// </remarks>
    public static class KvStringParser
    {
        /// <summary>
        ///     Tokenizes <paramref name="raw"/> by any character in
        ///     <paramref name="separators"/>, then yields each non-empty token
        ///     as a (key, value) pair split on the first <c>'='</c>.
        ///
        ///     <para>
        ///         Empty tokens are skipped. Tokens without <c>'='</c>, or with
        ///         <c>'='</c> at position 0 (empty key), throw
        ///         <see cref="ArgumentException"/> with a message referencing
        ///         <paramref name="contextLabel"/> when supplied.
        ///     </para>
        /// </summary>
        /// <param name="raw">Input string to tokenize.</param>
        /// <param name="separators">
        ///     Characters treated as token separators (e.g., <c>[',']</c> for
        ///     comma-separated values or <c>[' ', '\t', '\r', '\n']</c> for
        ///     whitespace-separated values).
        /// </param>
        /// <param name="contextLabel">
        ///     Optional label woven into error messages to identify which
        ///     config the malformed token came from (e.g.
        ///     <c>"sasl.oauthbearer.extensions"</c>). When <c>null</c>, error
        ///     messages use a generic <c>"key=value entry"</c> phrasing.
        /// </param>
        /// <param name="trimTokens">
        ///     When <c>true</c> (default), each token is trimmed of leading
        ///     and trailing whitespace before being split on <c>'='</c> —
        ///     matching librdkafka's <c>rd_string_split</c> behaviour. Set to
        ///     <c>false</c> for strict tokenization that preserves all
        ///     whitespace inside tokens.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <paramref name="raw"/> or <paramref name="separators"/> is null.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     A token is malformed — no <c>'='</c> separator, or empty key.
        /// </exception>
        public static IEnumerable<KeyValuePair<string, string>> Parse(
            string raw,
            char[] separators,
            string contextLabel = null,
            bool trimTokens = true)
        {
            if (raw == null) throw new ArgumentNullException(nameof(raw));
            if (separators == null) throw new ArgumentNullException(nameof(separators));

            foreach (var rawToken in raw.Split(separators, StringSplitOptions.RemoveEmptyEntries))
            {
                var token = trimTokens ? rawToken.Trim() : rawToken;
                if (token.Length == 0) continue;

                var idx = token.IndexOf('=');
                if (idx <= 0)
                {
                    var what = contextLabel != null
                        ? $"'{contextLabel}'"
                        : "key=value";
                    throw new ArgumentException(
                        $"Malformed {what} entry '{token}' (expected key=value).");
                }
                yield return new KeyValuePair<string, string>(
                    token.Substring(0, idx),
                    token.Substring(idx + 1));
            }
        }
    }
}
