// Copyright 2022 Confluent Inc.
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

using System.Text;
using System.Text.RegularExpressions;

namespace Confluent.SchemaRegistry
{
    public class WildcardMatcher
    {
        /// <summary>
        /// Matches fully-qualified names that use dot (.) as the name boundary.
        /// </summary>
        /// <param name="str">
        ///     The string to match on.
        /// </param>
        /// <param name="wildcardMatcher">
        ///     The wildcard string to match against.
        /// </param>
        public static bool Match(string str, string wildcardMatcher)
        {
            if (str == null && wildcardMatcher == null)
            {
                return true;
            }

            if (str == null || wildcardMatcher == null)
            {
                return false;
            }

            Regex wildcardRegexp = new Regex(WildcardToRegexp(wildcardMatcher, '.'));
            Match match = wildcardRegexp.Match(str);
            return match.Success && match.Value.Length == str.Length;
        }

        private static string WildcardToRegexp(string globExp, char separator)
        {
            StringBuilder dst = new StringBuilder();
            dst.Append("^");
            char[] src = globExp.Replace("**" + separator + "*", "**").ToCharArray();
            int i = 0;
            while (i < src.Length)
            {
                char c = src[i++];
                switch (c)
                {
                    case '*':
                        // One char lookahead for **
                        if (i < src.Length && src[i] == '*')
                        {
                            dst.Append(".*");
                            ++i;
                        }
                        else
                        {
                            dst.Append("[^");
                            dst.Append(separator);
                            dst.Append("]*");
                        }

                        break;
                    case '?':
                        dst.Append("[^");
                        dst.Append(separator);
                        dst.Append("]");
                        break;
                    case '.':
                    case '+':
                    case '{':
                    case '}':
                    case '(':
                    case ')':
                    case '|':
                    case '^':
                    case '$':
                        // These need to be escaped in regular expressions
                        dst.Append('\\').Append(c);
                        break;
                    case '\\':
                        i = DoubleSlashes(dst, src, i);
                        break;
                    default:
                        dst.Append(c);
                        break;
                }
            }

            dst.Append("$");
            return dst.ToString();
        }

        private static int DoubleSlashes(StringBuilder dst, char[] src, int i)
        {
            // Emit the next character without special interpretation
            dst.Append('\\');
            if ((i + 1) < src.Length)
            {
                dst.Append('\\');
                dst.Append(src[i]);
                i++;
            }
            else
            {
                // A backslash at the very end is treated like an escaped backslash
                dst.Append('\\');
            }

            return i;
        }
    }
}