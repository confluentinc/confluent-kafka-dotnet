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
using System.Text;

namespace Confluent.Kafka.Internal
{
    /// <summary>
    ///     Parses character-separated <c>key=value</c> strings with the same rules
    ///     as librdkafka's <c>rd_string_split</c> + <c>rd_kafka_conf_kv_split</c>, so
    ///     OAUTHBEARER config/extension strings tokenize identically to the native
    ///     client (e.g. an <c>identityPoolId</c> that is itself a comma-separated
    ///     list with backslash-quoted commas).
    /// </summary>
    /// <remarks>
    ///     Semantics (logically equivalent to librdkafka):
    ///     <list type="bullet">
    ///       <item>Fields are separated by a single <c>sep</c> character.</item>
    ///       <item><c>\</c> escapes the next character: <c>\t</c>/<c>\n</c>/<c>\r</c>/<c>\0</c>
    ///         map to TAB/LF/CR/NUL; any other escaped character (including an escaped
    ///         separator or <c>\\</c>) is kept literally.</item>
    ///       <item>Leading whitespace is stripped only when unescaped; trailing
    ///         whitespace is stripped unconditionally. "Whitespace" is the ASCII set
    ///         (C <c>isspace</c>): space, <c>\t</c>, <c>\n</c>, <c>\v</c>, <c>\f</c>,
    ///         <c>\r</c> — deliberately not Unicode.</item>
    ///       <item>Empty fields are skipped when <c>skipEmpty</c> is set.</item>
    ///       <item><c>key=value</c> is split on the FIRST <c>'='</c> (the value may
    ///         contain further <c>'='</c>); a field with no <c>'='</c> or an empty key
    ///         is an error.</item>
    ///     </list>
    /// </remarks>
    internal static class LibrdkafkaStringParser
    {
        /// <summary>
        ///     ASCII <c>isspace</c>: space, <c>\t</c>(0x09)..<c>\r</c>(0x0D). Matches
        ///     librdkafka's C behaviour; intentionally not <see cref="char.IsWhiteSpace(char)"/>,
        ///     which also matches Unicode whitespace and would diverge.
        /// </summary>
        private static bool IsAsciiSpace(char c) => c == ' ' || (c >= '\t' && c <= '\r');

        /// <summary>
        ///     Splits <paramref name="input"/> into fields on <paramref name="sep"/>,
        ///     applying librdkafka's <c>\</c>-escaping and whitespace trimming.
        ///     Logically equivalent to librdkafka's <c>rd_string_split</c>.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="input"/> is null.</exception>
        internal static List<string> Split(string input, char sep, bool skipEmpty)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));

            var fields = new List<string>();
            var field = new StringBuilder();
            var nextEsc = false;

            for (var idx = 0; idx <= input.Length; idx++)
            {
                var atEnd = idx == input.Length;
                var isEsc = nextEsc;

                if (!atEnd)
                {
                    var c = input[idx];

                    // An unescaped backslash is consumed and escapes the next char.
                    if (!isEsc && c == '\\')
                    {
                        nextEsc = true;
                        continue;
                    }

                    nextEsc = false;

                    // Strip leading whitespace (only when unescaped).
                    if (!isEsc && field.Length == 0 && IsAsciiSpace(c))
                    {
                        continue;
                    }

                    // Content char: any escaped char, or any non-separator char.
                    if (isEsc || c != sep)
                    {
                        if (isEsc)
                        {
                            switch (c)
                            {
                                case 't': c = '\t'; break;
                                case 'n': c = '\n'; break;
                                case 'r': c = '\r'; break;
                                case '0': c = '\0'; break;
                                // Any other escaped char (e.g. \, or \\) is kept as-is.
                            }
                        }

                        field.Append(c);
                        continue;
                    }

                    // Otherwise c is an unescaped separator: fall through and finish
                    // the current field.
                }

                // Finish the current field (reached on a separator or end-of-input).
                while (field.Length > 0 && IsAsciiSpace(field[field.Length - 1]))
                {
                    field.Length--; // strip trailing whitespace
                }

                if (field.Length == 0 && skipEmpty)
                {
                    if (atEnd) break;
                    continue;
                }

                fields.Add(field.ToString());
                field.Clear();

                if (atEnd) break;
            }

            return fields;
        }

        /// <summary>
        ///     Splits <paramref name="input"/> via <see cref="Split"/> (skipping empty
        ///     fields) and parses each field into a key/value pair on its first
        ///     <c>'='</c>. Logically equivalent to applying librdkafka's
        ///     <c>rd_kafka_conf_kv_get</c> over every pair.
        /// </summary>
        /// <param name="input">The raw key=value string to parse.</param>
        /// <param name="sep">The single field-separator character (e.g. <c>','</c>).</param>
        /// <param name="contextLabel">
        ///     Label woven into the error message to identify which config a malformed
        ///     entry came from (e.g. <c>SaslOauthbearerConfig</c>).
        /// </param>
        /// <exception cref="ArgumentNullException"><paramref name="input"/> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     A field has no <c>'='</c>, or has an empty key.
        /// </exception>
        internal static List<KeyValuePair<string, string>> ParseKeyValues(
            string input, char sep, string contextLabel)
        {
            var pairs = new List<KeyValuePair<string, string>>();

            foreach (var field in Split(input, sep, skipEmpty: true))
            {
                var eq = field.IndexOf('=');
                if (eq <= 0)
                {
                    throw new ArgumentException(
                        $"Malformed entry '{field}' in {contextLabel} (expected key=value).");
                }

                pairs.Add(new KeyValuePair<string, string>(
                    field.Substring(0, eq),
                    field.Substring(eq + 1)));
            }

            return pairs;
        }
    }
}
