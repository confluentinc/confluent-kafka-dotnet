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

using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Pins which inline-rule option syntaxes protobuf-net's .proto parser accepts.
    ///
    ///     Since protobuf-net 3.3.21 the bracketed list form used by the JVM, Go, Python and
    ///     JavaScript clients (<c>rules: [{...}]</c>) is parsed correctly, including multiple
    ///     rules per field or message. That is the canonical cross-client form and what these
    ///     tests and the fixtures use. The single-value block form (<c>rules { ... }</c>) also
    ///     still parses, but repeating it keeps only the last rule, so it must not be used for
    ///     more than one rule.
    /// </summary>
    public class ProtobufRulesSyntaxTests
    {
        private static global::Confluent.SchemaRegistry.Serdes.Protobuf.Meta ParseFieldMeta(
            string optionBody)
        {
            string schema = @"
                syntax = ""proto3"";
                package example;
                import ""confluent/meta.proto"";
                message M {
                  int32 n = 1 [(.confluent.field_meta) = { " + optionBody + @" }];
                }
            ";
            var fds = ProtobufUtils.Parse(schema, new Dictionary<string, string>());
            var file = fds.Files.First(f => f.Name == "__root.proto");
            var msg = file.MessageTypes.FirstOrDefault();
            if (msg == null || msg.Fields.Count == 0)
            {
                return null;
            }

            return ProtobufUtils.GetMeta(msg.Fields[0].Options);
        }

        [Theory]
        [InlineData(@"rules: [{name: ""r"" expr: ""this >= 0""}]")]
        [InlineData(@"rules: [{name: ""r"", expr: ""this >= 0""}]")]
        [InlineData(@"rules { name: ""r"" expr: ""this >= 0"" }")]
        [InlineData(@"rules: { name: ""r"" expr: ""this >= 0"" }")]
        public void SingleRuleIsParsedAndRead(string optionBody)
        {
            var meta = ParseFieldMeta(optionBody);
            Assert.NotNull(meta);
            Assert.Single(meta.Rules);
            Assert.Equal("r", meta.Rules[0].Name);
        }

        [Fact]
        public void BracketedListRetainsEveryFieldRule()
        {
            // The cross-client form: every rule in the list is kept, in order.
            var meta = ParseFieldMeta(
                @"rules: [{name: ""a"", expr: ""this >= 0""}, {name: ""b"", expr: ""this <= 150""}]");
            Assert.NotNull(meta);
            Assert.Equal(new List<string> { "a", "b" }, meta.Rules.Select(r => r.Name).ToList());
        }

        [Fact]
        public void BracketedListRetainsEveryMessageRule()
        {
            string schema = @"
                syntax = ""proto3"";
                package example;
                import ""confluent/meta.proto"";
                message M {
                  option (.confluent.message_meta) = {
                    rules: [{name: ""m1"", expr: ""true""}, {name: ""m2"", expr: ""true""}]
                  };
                  int32 n = 1;
                }
            ";
            var fds = ProtobufUtils.Parse(schema, new Dictionary<string, string>());
            var file = fds.Files.First(f => f.Name == "__root.proto");
            var msg = file.MessageTypes.First();
            var meta = ProtobufUtils.GetMeta(msg.Options);
            Assert.NotNull(meta);
            Assert.Equal(new List<string> { "m1", "m2" }, meta.Rules.Select(r => r.Name).ToList());
        }

        [Fact]
        public void TagsOptionIsParsedAndRead()
        {
            var meta = ParseFieldMeta(@"tags: ""PII""");
            Assert.NotNull(meta);
            Assert.Equal(new List<string> { "PII" }, meta.Tags.ToList());
        }
    }
}
