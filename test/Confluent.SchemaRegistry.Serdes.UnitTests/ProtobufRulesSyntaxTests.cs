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
    ///     The bracketed list form used by the JVM, Python and JavaScript clients
    ///     (<c>rules: [{...}]</c>) is NOT supported: the parser fails and silently yields a
    ///     message with no fields, so rules are invisible. Rules must be written as repeated
    ///     <c>rules { ... }</c> blocks instead.
    /// </summary>
    public class ProtobufRulesSyntaxTests
    {
        private static (int fields, int rules) ParseFieldOption(string optionBody)
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
            int fields = msg?.Fields.Count ?? 0;
            if (fields == 0)
            {
                return (0, 0);
            }

            var meta = ProtobufUtils.GetMeta(msg.Fields[0].Options);
            return (fields, meta?.Rules.Count ?? 0);
        }

        [Theory]
        [InlineData(@"rules { name: ""r"" expr: ""this >= 0"" }")]
        [InlineData(@"rules: { name: ""r"" expr: ""this >= 0"" }")]
        public void SupportedRuleSyntaxIsParsedAndRead(string optionBody)
        {
            var (fields, rules) = ParseFieldOption(optionBody);
            Assert.Equal(1, fields);
            Assert.Equal(1, rules);
        }

        [Theory]
        [InlineData(@"rules: [{name: ""r"" expr: ""this >= 0""}]")]
        [InlineData(@"rules: [{name: ""r"", expr: ""this >= 0""}]")]
        public void BracketedListSyntaxIsNotSupportedByTheParser(string optionBody)
        {
            // Documents a protobuf-net limitation, not desired behavior: the parse fails and
            // the message comes back with no fields at all, rather than raising an error.
            var (fields, rules) = ParseFieldOption(optionBody);
            Assert.Equal(0, fields);
            Assert.Equal(0, rules);
        }

        [Fact]
        public void TagsOptionIsParsedAndRead()
        {
            string schema = @"
                syntax = ""proto3"";
                package example;
                import ""confluent/meta.proto"";
                message M {
                  int32 n = 1 [(.confluent.field_meta) = { tags: ""PII"" }];
                }
            ";
            var fds = ProtobufUtils.Parse(schema, new Dictionary<string, string>());
            var file = fds.Files.First(f => f.Name == "__root.proto");
            var msg = file.MessageTypes.First();
            var meta = ProtobufUtils.GetMeta(msg.Fields[0].Options);
            Assert.NotNull(meta);
            Assert.Equal(new List<string> { "PII" }, meta.Tags.ToList());
        }

        [Fact]
        public void OnlyTheLastRuleSurvivesWhenRepeatingBlocks()
        {
            // Documents a protobuf-net limitation, not desired behavior: repeated
            // `rules { ... }` blocks parse, but each one overwrites the previous instead of
            // appending, so only the last rule is kept — silently. In practice .NET supports
            // at most one inline rule per field.
            var (fields, rules) = ParseFieldOption(
                @"rules { name: ""a"" expr: ""this >= 0"" } rules { name: ""b"" expr: ""this <= 150"" }");
            Assert.Equal(1, fields);
            Assert.Equal(1, rules);
        }

        [Fact]
        public void OnlyTheLastMessageRuleSurvivesWhenRepeatingBlocks()
        {
            string schema = @"
                syntax = ""proto3"";
                package example;
                import ""confluent/meta.proto"";
                message M {
                  option (.confluent.message_meta) = {
                    rules { name: ""m1"" expr: ""true"" }
                    rules { name: ""m2"" expr: ""true"" }
                  };
                  int32 n = 1;
                }
            ";
            var fds = ProtobufUtils.Parse(schema, new Dictionary<string, string>());
            var file = fds.Files.First(f => f.Name == "__root.proto");
            var msg = file.MessageTypes.First();
            Assert.Equal(1, msg.Fields.Count);
            var meta = ProtobufUtils.GetMeta(msg.Options);
            Assert.NotNull(meta);
            // Same last-wins truncation at the message level.
            Assert.Equal(new List<string> { "m2" }, meta.Rules.Select(r => r.Name).ToList());
        }

    }
}
