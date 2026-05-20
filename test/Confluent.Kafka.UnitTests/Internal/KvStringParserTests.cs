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
using System.Linq;
using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests.Internal
{
    public class KvStringParserTests
    {
        // ---- Null guards ----

        [Fact]
        public void Parse_NullRaw_Throws()
        {
            Assert.Throws<ArgumentNullException>(
                () => KvStringParser.Parse(null, new[] { ',' }).ToList());
        }

        [Fact]
        public void Parse_NullSeparators_Throws()
        {
            Assert.Throws<ArgumentNullException>(
                () => KvStringParser.Parse("a=1", null).ToList());
        }

        // ---- Empty / whitespace-only input ----

        [Fact]
        public void Parse_EmptyRaw_YieldsNothing()
        {
            Assert.Empty(KvStringParser.Parse("", new[] { ',' }));
        }

        [Fact]
        public void Parse_WhitespaceOnlyRaw_YieldsNothing()
        {
            // After split + trim, no real tokens remain.
            Assert.Empty(KvStringParser.Parse("   ", new[] { ',' }));
        }

        // ---- Single separator ----

        [Fact]
        public void Parse_SingleSeparator_CommaBasicCase()
        {
            var pairs = KvStringParser.Parse("a=1,b=2", new[] { ',' }).ToList();
            Assert.Equal(2, pairs.Count);
            Assert.Equal("a", pairs[0].Key); Assert.Equal("1", pairs[0].Value);
            Assert.Equal("b", pairs[1].Key); Assert.Equal("2", pairs[1].Value);
        }

        // ---- Multiple separators (whitespace-tolerant case) ----

        [Fact]
        public void Parse_MultipleSeparators_AllRecognized()
        {
            var pairs = KvStringParser.Parse(
                "a=1\tb=2\nc=3 d=4",
                new[] { ' ', '\t', '\r', '\n' }).ToList();
            Assert.Equal(4, pairs.Count);
            Assert.Equal("a", pairs[0].Key);
            Assert.Equal("b", pairs[1].Key);
            Assert.Equal("c", pairs[2].Key);
            Assert.Equal("d", pairs[3].Key);
        }

        // ---- Empty tokens skipped ----

        [Fact]
        public void Parse_ConsecutiveSeparators_SkipEmptyTokens()
        {
            var pairs = KvStringParser.Parse("a=1,,b=2,", new[] { ',' }).ToList();
            Assert.Equal(2, pairs.Count);
        }

        // ---- Trim toggle ----

        [Fact]
        public void Parse_TrimTokensDefaultTrue_StripsLeadingAndTrailing()
        {
            // Matches librdkafka rd_string_split (rdstring.c:493-525) —
            // strips both leading and trailing whitespace per element.
            var pairs = KvStringParser.Parse(
                " a=1 , b=2 ",
                new[] { ',' }).ToList();
            Assert.Equal(2, pairs.Count);
            Assert.Equal("a", pairs[0].Key); Assert.Equal("1", pairs[0].Value);
            Assert.Equal("b", pairs[1].Key); Assert.Equal("2", pairs[1].Value);
        }

        [Fact]
        public void Parse_TrimTokensFalse_PreservesWhitespaceInsideTokens()
        {
            var pairs = KvStringParser.Parse(
                " a=1 ,b=2 ",
                new[] { ',' },
                trimTokens: false).ToList();
            Assert.Equal(2, pairs.Count);
            Assert.Equal(" a", pairs[0].Key);
            Assert.Equal("1 ", pairs[0].Value);
            Assert.Equal("b", pairs[1].Key);
            Assert.Equal("2 ", pairs[1].Value);
        }

        // ---- Malformed tokens ----

        [Fact]
        public void Parse_TokenWithoutEquals_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => KvStringParser.Parse("a=1,bad,c=3", new[] { ',' }).ToList());
            Assert.Contains("Malformed", ex.Message);
            Assert.Contains("bad", ex.Message);
        }

        [Fact]
        public void Parse_TokenStartsWithEquals_Throws()
        {
            // idx == 0 → empty key.
            var ex = Assert.Throws<ArgumentException>(
                () => KvStringParser.Parse("=value", new[] { ',' }).ToList());
            Assert.Contains("Malformed", ex.Message);
        }

        // ---- Value-content semantics ----

        [Fact]
        public void Parse_ValueContainsEquals_KeptVerbatim()
        {
            // Split is on FIRST '=' only — the rest stays in the value.
            var pairs = KvStringParser.Parse("key=val=ue", new[] { ',' }).ToList();
            Assert.Single(pairs);
            Assert.Equal("key", pairs[0].Key);
            Assert.Equal("val=ue", pairs[0].Value);
        }

        [Fact]
        public void Parse_EmptyValue_Allowed()
        {
            // RFC 7628 SASL extensions allow empty values; AWS allows empty tag values.
            var pairs = KvStringParser.Parse("key=", new[] { ',' }).ToList();
            Assert.Single(pairs);
            Assert.Equal("key", pairs[0].Key);
            Assert.Equal("", pairs[0].Value);
        }

        // ---- Duplicate keys preserved (caller's responsibility to dedupe) ----

        [Fact]
        public void Parse_DuplicateKeys_BothYielded()
        {
            var pairs = KvStringParser.Parse("a=1,a=2", new[] { ',' }).ToList();
            Assert.Equal(2, pairs.Count);
            Assert.Equal("1", pairs[0].Value);
            Assert.Equal("2", pairs[1].Value);
        }

        // ---- contextLabel weaves into error messages ----

        [Fact]
        public void Parse_ContextLabelPresent_AppearsInErrorMessage()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => KvStringParser.Parse(
                    "bad",
                    new[] { ',' },
                    contextLabel: "my.config.key").ToList());
            Assert.Contains("my.config.key", ex.Message);
        }

        [Fact]
        public void Parse_ContextLabelNull_UsesGenericErrorPhrase()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => KvStringParser.Parse("bad", new[] { ',' }).ToList());
            Assert.Contains("key=value", ex.Message);
        }
    }
}
