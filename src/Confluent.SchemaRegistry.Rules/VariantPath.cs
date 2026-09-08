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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using SrVariant = Confluent.SchemaRegistry.Variant;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     The JSONPath subset used by <c>variants.path(v, path)</c> — a port of Java's
    ///     <c>VariantPath</c> / the Python and JS clients. Supports:
    ///     <list type="bullet">
    ///         <item><description><c>$</c> — root</description></item>
    ///         <item><description><c>$.field</c> / <c>$.field.subfield</c> — object field by
    ///             identifier name</description></item>
    ///         <item><description><c>$[i]</c> — array element by non-negative integer index</description></item>
    ///         <item><description><c>$["quoted key"]</c> / <c>$['quoted key']</c> — quoted key
    ///             for non-identifier names</description></item>
    ///     </list>
    ///     <para>
    ///         Resolution failures (missing field, out-of-bounds index, type mismatch) return
    ///         <c>null</c> from <see cref="Walk" />; malformed paths throw
    ///         <see cref="System.ArgumentException" /> at parse time. Identifier names follow
    ///         <c>[A-Za-z_][A-Za-z0-9_]*</c>; use the quoted form for any other key. Negative
    ///         indices are rejected (no RFC 9535 <c>len + i</c> semantics). Quoted-key escapes
    ///         recognize only <c>\\</c> (a literal backslash) and backslash + the enclosing
    ///         quote; any other escape is a parse error rather than being silently decoded
    ///         (option B).
    ///     </para>
    /// </summary>
    internal static class VariantPath
    {
        // Bounded cache of parsed paths: rules usually pass a literal path that recurs per
        // record. Only successful parses are cached, so a malformed path throws every call.
        private const int MaxCachedPaths = 1000;

        private static readonly ConcurrentDictionary<string, IReadOnlyList<Segment>> ParseCache =
            new ConcurrentDictionary<string, IReadOnlyList<Segment>>();

        /// <summary>
        ///     Walk <paramref name="root" /> following <paramref name="path" />. Returns the
        ///     resolved Variant, or <c>null</c> if any segment fails to resolve. Throws on a
        ///     malformed path.
        /// </summary>
        public static SrVariant Walk(SrVariant root, string path)
        {
            SrVariant current = root;
            foreach (Segment seg in Parse(path))
            {
                if (current == null)
                {
                    return null;
                }

                if (seg.IsIndex)
                {
                    current = current.GetVariantType() == VariantType.Array
                        ? current.GetElementAtIndex(seg.Index)
                        : null;
                }
                else
                {
                    current = current.GetVariantType() == VariantType.Object
                        ? current.GetFieldByKey(seg.Key)
                        : null;
                }
            }

            return current;
        }

        private static IReadOnlyList<Segment> Parse(string path)
        {
            if (path == null)
            {
                throw new System.ArgumentException("variant path must start with '$'");
            }

            if (ParseCache.TryGetValue(path, out var cached))
            {
                return cached;
            }

            IReadOnlyList<Segment> parsed = ParseInternal(path);
            // Bounded like the reference's Guava cache (maximumSize(1000)): variants.path takes
            // a runtime string, so a rule building paths by interpolation would otherwise retain
            // one entry per distinct path forever. Clearing on overflow is cruder than Guava's
            // eviction but keeps the ceiling; the paths that matter are re-parsed once.
            if (ParseCache.Count >= MaxCachedPaths)
            {
                ParseCache.Clear();
            }

            ParseCache[path] = parsed;
            return parsed;
        }

        private static IReadOnlyList<Segment> ParseInternal(string path)
        {
            if (path.Length == 0)
            {
                throw new System.ArgumentException("variant path must start with '$'");
            }

            var c = new Cursor(path);
            if (c.Peek() != '$')
            {
                throw new System.ArgumentException("variant path must start with '$', got: " + path);
            }

            c.Next();
            var outSegs = new List<Segment>();
            while (c.HasMore())
            {
                char ch = c.Peek();
                if (ch == '.')
                {
                    c.Next();
                    outSegs.Add(Segment.Field(ReadIdent(c, path)));
                }
                else if (ch == '[')
                {
                    c.Next();
                    if (!c.HasMore())
                    {
                        throw new System.ArgumentException(
                            "unexpected end of input after '[' in variant path: " + path);
                    }

                    if (c.Peek() == '"' || c.Peek() == '\'')
                    {
                        outSegs.Add(Segment.Field(ReadQuotedKey(c, path)));
                    }
                    else
                    {
                        outSegs.Add(Segment.Idx(ReadIndex(c, path)));
                    }

                    if (!c.HasMore() || c.Next() != ']')
                    {
                        throw new System.ArgumentException("expected ']' in variant path: " + path);
                    }
                }
                else
                {
                    throw new System.ArgumentException(
                        "unexpected character '" + ch + "' in variant path: " + path);
                }
            }

            return outSegs;
        }

        private static string ReadIdent(Cursor c, string path)
        {
            if (!c.HasMore() || !(char.IsLetter(c.Peek()) || c.Peek() == '_'))
            {
                throw new System.ArgumentException(
                    "expected identifier (starting with a letter or '_') after '.' in variant path: "
                    + path);
            }

            int start = c.Pos;
            c.Next();
            while (c.HasMore())
            {
                char ch = c.Peek();
                if (char.IsLetterOrDigit(ch) || ch == '_')
                {
                    c.Next();
                }
                else
                {
                    break;
                }
            }

            return c.Src.Substring(start, c.Pos - start);
        }

        private static string ReadQuotedKey(Cursor c, string path)
        {
            char quote = c.Next();
            var sb = new StringBuilder();
            while (c.HasMore())
            {
                char ch = c.Next();
                if (ch == '\\')
                {
                    // Option B: only '\\' (literal backslash) and backslash + the enclosing
                    // quote are recognized. Any other escape — including a would-be Unicode
                    // escape — is a parse error rather than being silently decoded.
                    if (!c.HasMore())
                    {
                        throw new System.ArgumentException(
                            "unterminated escape at end of quoted key in variant path: " + path);
                    }

                    char esc = c.Next();
                    if (esc == '\\' || esc == quote)
                    {
                        sb.Append(esc);
                    }
                    else
                    {
                        throw new System.ArgumentException(
                            "unsupported escape '\\" + esc + "' in quoted key of variant path (only "
                            + "'\\\\' and '\\" + quote + "' are allowed): " + path);
                    }
                }
                else if (ch == quote)
                {
                    return sb.ToString();
                }
                else
                {
                    sb.Append(ch);
                }
            }

            throw new System.ArgumentException("unterminated quoted key in variant path: " + path);
        }

        private static int ReadIndex(Cursor c, string path)
        {
            if (c.HasMore() && c.Peek() == '-')
            {
                throw new System.ArgumentException(
                    "negative indices are not supported in variant path: " + path);
            }

            int start = c.Pos;
            while (c.HasMore() && char.IsDigit(c.Peek()))
            {
                c.Next();
            }

            if (c.Pos == start)
            {
                throw new System.ArgumentException("expected integer index in variant path: " + path);
            }

            if (!int.TryParse(c.Src.Substring(start, c.Pos - start), out int index))
            {
                throw new System.ArgumentException("index out of int range in variant path: " + path);
            }

            return index;
        }

        private readonly struct Segment
        {
            public bool IsIndex { get; }

            public string Key { get; }

            public int Index { get; }

            private Segment(bool isIndex, string key, int index)
            {
                IsIndex = isIndex;
                Key = key;
                Index = index;
            }

            public static Segment Field(string key) => new Segment(false, key, 0);

            public static Segment Idx(int index) => new Segment(true, null, index);
        }

        private sealed class Cursor
        {
            public string Src { get; }

            public int Pos { get; private set; }

            public Cursor(string src)
            {
                Src = src;
            }

            public bool HasMore() => Pos < Src.Length;

            public char Peek() => Src[Pos];

            public char Next() => Src[Pos++];
        }
    }
}
