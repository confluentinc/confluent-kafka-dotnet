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
using Xunit;

namespace Confluent.Kafka.UnitTests.Internal
{
    public class LibrdkafkaStringParserTests
    {
        // ---- Ported verbatim from librdkafka's ut_string_split (src/rdstring.c). ----
        // These lock logical parity with rd_string_split. Verbatim C# strings keep
        // '\' literal, matching librdkafka's test inputs.

        [Fact]
        public void Split_SingleField()
            => Assert.Equal(
                new[] { "just one field" },
                LibrdkafkaStringParser.Split("just one field", ',', skipEmpty: true));

        [Fact]
        public void Split_Empty_SkipEmpty_NoFields()
            => Assert.Equal(
                Array.Empty<string>(),
                LibrdkafkaStringParser.Split("", ',', skipEmpty: true));

        [Fact]
        public void Split_Empty_NoSkipEmpty_OneEmptyField()
            => Assert.Equal(
                new[] { "" },
                LibrdkafkaStringParser.Split("", ',', skipEmpty: false));

        [Fact]
        public void Split_WhitespaceAndEmpties_SkipEmpty()
            => Assert.Equal(
                new[] { "a", "b", "c", "d", "e", "f", "ghijk", "lmn", "opq", "r  s t u", "v" },
                LibrdkafkaStringParser.Split(
                    ", a,b ,,c,   d,    e,f,ghijk,  lmn,opq  ,  r  s t u, v",
                    ',', skipEmpty: true));

        [Fact]
        public void Split_WhitespaceAndEmpties_NoSkipEmpty()
            => Assert.Equal(
                new[] { "", "a", "b", "", "c", "d", "e", "f", "ghijk", "lmn", "opq", "r  s t u", "v" },
                LibrdkafkaStringParser.Split(
                    ", a,b ,,c,   d,    e,f,ghijk,  lmn,opq  ,  r  s t u, v",
                    ',', skipEmpty: false));

        [Fact]
        public void Split_Escapes_QuotedSeparatorsAndBackslashes()
            => Assert.Equal(
                new[] { "this is an ,escaped comma", ",", @"\", @"and this is an unbalanced escape: \\\" },
                LibrdkafkaStringParser.Split(
                    @"  this is an \,escaped comma,\,,\\, and this is an unbalanced escape: \\\\\\\",
                    ',', skipEmpty: true));

        [Fact]
        public void Split_AlternateSeparator_Pipe_NoSkipEmpty()
            => Assert.Equal(
                new[] { "using", "another", "", "|d", "elimiter" },
                LibrdkafkaStringParser.Split(@"using|another ||\|d|elimiter", '|', skipEmpty: false));

        // ---- Logical-parity gotchas ----

        [Fact]
        public void Split_EscapeSubstitutions_TabNewlineCr()
            // \t \n \r substitute to TAB/LF/CR; kept internal so trailing-trim won't touch them.
            => Assert.Equal(
                new[] { "a\tb\nc\rd" },
                LibrdkafkaStringParser.Split(@"a\tb\nc\rd", ',', skipEmpty: true));

        [Fact]
        public void Split_UnicodeWhitespaceNotTrimmed()
        {
            // U+00A0 (NBSP) is Unicode whitespace but NOT ASCII isspace, so neither the
            // leading nor trailing NBSP is trimmed -- proving we match C isspace, not
            // char.IsWhiteSpace (which WOULD trim them, diverging from librdkafka).
            Assert.Equal(
                new[] { " a " },
                LibrdkafkaStringParser.Split(" a ", ',', skipEmpty: true));
        }

        [Fact]
        public void Split_EscapedWhitespace_LeadingKept_TrailingTrimmed()
        {
            // Asymmetry copied from librdkafka: leading whitespace is stripped only when
            // UNescaped, so an escaped \t at the start survives; trailing whitespace is
            // stripped UNCONDITIONALLY, so an escaped \t at the end is removed.
            Assert.Equal(new[] { "\tx" }, LibrdkafkaStringParser.Split(@"\tx", ',', skipEmpty: true));
            Assert.Equal(new[] { "x" }, LibrdkafkaStringParser.Split(@"x\t", ',', skipEmpty: true));
        }

        [Fact]
        public void Split_WhitespaceOnly_TrimsToEmpty()
        {
            // An all-whitespace field trims to empty (distinct path from ""): skipped
            // when skipEmpty, else returned as a single empty field.
            Assert.Equal(Array.Empty<string>(), LibrdkafkaStringParser.Split("   ", ',', skipEmpty: true));
            Assert.Equal(new[] { "" }, LibrdkafkaStringParser.Split("   ", ',', skipEmpty: false));
        }

        // ---- ParseKeyValues (rd_kafka_conf_kv_split semantics) ----

        [Fact]
        public void ParseKeyValues_BasicPairs()
            => Assert.Equal(
                new[]
                {
                    new KeyValuePair<string, string>("region", "us-east-1"),
                    new KeyValuePair<string, string>("audience", "https://x"),
                },
                LibrdkafkaStringParser.ParseKeyValues("region=us-east-1,audience=https://x", ',', "cfg"));

        [Fact]
        public void ParseKeyValues_SplitsOnFirstEquals()
            => Assert.Equal(
                new[] { new KeyValuePair<string, string>("a", "b=c") },
                LibrdkafkaStringParser.ParseKeyValues("a=b=c", ',', "cfg"));

        [Fact]
        public void ParseKeyValues_EmptyValueAllowed()
            => Assert.Equal(
                new[] { new KeyValuePair<string, string>("a", "") },
                LibrdkafkaStringParser.ParseKeyValues("a=", ',', "cfg"));

        [Fact]
        public void ParseKeyValues_EscapedCommaInValue_StaysOneEntry()
            // Headline case: identityPoolId as a comma-separated list, commas quoted with '\'.
            => Assert.Equal(
                new[] { new KeyValuePair<string, string>("identityPoolId", "pool-1,pool-2") },
                LibrdkafkaStringParser.ParseKeyValues(@"identityPoolId=pool-1\,pool-2", ',', "cfg"));

        [Fact]
        public void ParseKeyValues_NoEquals_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => LibrdkafkaStringParser.ParseKeyValues("abc", ',', "cfg"));
            Assert.Contains("cfg", ex.Message);
        }

        [Fact]
        public void ParseKeyValues_EmptyKey_Throws()
            => Assert.Throws<ArgumentException>(
                () => LibrdkafkaStringParser.ParseKeyValues("=value", ',', "cfg"));

        [Fact]
        public void ParseKeyValues_FullConfig_AllPairsInOrder()
        {
            // A realistic, complete SaslOauthbearerConfig: every supported key,
            // comma-separated, with URL values (colons/slashes are ordinary content).
            var pairs = LibrdkafkaStringParser.ParseKeyValues(
                "region=us-east-1,audience=https://a," +
                "duration_seconds=900,signing_algorithm=RS256," +
                "sts_endpoint=https://sts.us-east-1.amazonaws.com," +
                "aws_debug=none," +
                "tag_team=platform,tag_environment=prod",
                ',', "SaslOauthbearerConfig");

            Assert.Equal(
                new[]
                {
                    new KeyValuePair<string, string>("region", "us-east-1"),
                    new KeyValuePair<string, string>("audience", "https://a"),
                    new KeyValuePair<string, string>("duration_seconds", "900"),
                    new KeyValuePair<string, string>("signing_algorithm", "RS256"),
                    new KeyValuePair<string, string>("sts_endpoint", "https://sts.us-east-1.amazonaws.com"),
                    new KeyValuePair<string, string>("aws_debug", "none"),
                    new KeyValuePair<string, string>("tag_team", "platform"),
                    new KeyValuePair<string, string>("tag_environment", "prod"),
                },
                pairs);
        }
    }
}
