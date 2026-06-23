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
using Confluent.Kafka.OAuthBearer.Aws.Internal;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class SaslExtensionsParserTests
    {
        // ---- Null / empty input → null ----

        [Fact]
        public void Parse_NullRaw_ReturnsNull()
        {
            Assert.Null(SaslExtensionsParser.Parse(null));
        }

        [Fact]
        public void Parse_EmptyString_ReturnsNull()
        {
            Assert.Null(SaslExtensionsParser.Parse(""));
        }

        // ---- Happy paths ----

        [Fact]
        public void Parse_SingleEntry_ReturnsOneItem()
        {
            var result = SaslExtensionsParser.Parse("logicalCluster=lkc-abc");
            Assert.NotNull(result);
            Assert.Single(result);
            Assert.Equal("lkc-abc", result["logicalCluster"]);
        }

        [Fact]
        public void Parse_MultipleEntries_ReturnsAll()
        {
            var result = SaslExtensionsParser.Parse(
                "logicalCluster=lkc-abc,identityPoolId=pool-x");
            Assert.Equal(2, result.Count);
            Assert.Equal("lkc-abc", result["logicalCluster"]);
            Assert.Equal("pool-x", result["identityPoolId"]);
        }

        [Fact]
        public void Parse_WhitespaceAroundCommas_TrimmedAndParsed()
        {
            // Each comma-delimited entry is trimmed before parsing.
            var result = SaslExtensionsParser.Parse(
                " logicalCluster=lkc-abc ,  identityPoolId=pool-x ");
            Assert.Equal(2, result.Count);
            Assert.Equal("lkc-abc", result["logicalCluster"]);
            Assert.Equal("pool-x", result["identityPoolId"]);
        }

        [Fact]
        public void Parse_EmptyEntries_Tolerated()
        {
            var result = SaslExtensionsParser.Parse(
                "logicalCluster=lkc-abc,,identityPoolId=pool-x,");
            Assert.Equal(2, result.Count);
            Assert.Equal("lkc-abc", result["logicalCluster"]);
            Assert.Equal("pool-x", result["identityPoolId"]);
        }

        [Fact]
        public void Parse_EmptyValue_Accepted()
        {
            // RFC 7628 SASL extensions allow empty values; mirror that.
            var result = SaslExtensionsParser.Parse("logicalCluster=");
            Assert.NotNull(result);
            Assert.Single(result);
            Assert.Equal("", result["logicalCluster"]);
        }

        [Fact]
        public void Parse_DuplicateKey_LastWins()
        {
            var result = SaslExtensionsParser.Parse("k=a,k=b");
            Assert.Single(result);
            Assert.Equal("b", result["k"]);
        }

        // ---- Malformed input → throws ----

        [Fact]
        public void Parse_MissingEquals_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => SaslExtensionsParser.Parse("noEqualsHere"));
            Assert.Contains("sasl.oauthbearer.extensions", ex.Message);
            Assert.Contains("Malformed", ex.Message);
        }

        [Fact]
        public void Parse_EmptyKey_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => SaslExtensionsParser.Parse("=value"));
            Assert.Contains("sasl.oauthbearer.extensions", ex.Message);
            Assert.Contains("Malformed", ex.Message);
        }
    }
}
