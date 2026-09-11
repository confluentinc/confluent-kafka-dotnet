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

using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Rules;
using Example;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Two result entries naming the same slot. Applying both left the outcome to whatever
    ///     order the rule wrote them in: for a oneof, setting a member clears its siblings, so
    ///     <c>{oneof_int32, oneof_string}</c> kept the second and the reverse kept the other.
    ///     <c>JsonFormat</c> - the reference's write-back path - refuses both shapes, with
    ///     <i>opposite</i> null handling, measured against protobuf-java:
    ///     <code>
    ///     {"oneofMessage":{..},"oneofString":"s"}   Cannot set field ...oneof_string because
    ///                                              another field ...oneof_message belonging to
    ///                                              the same oneof has already been set
    ///     {"oneofString":"s","oneofMessage":null}  OK - a null does not occupy the oneof
    ///     {"total_amount":{..},"totalAmount":{..}} Field ...total_amount has already been set.
    ///     {"total_amount":{..},"totalAmount":null} Field ...total_amount has already been set.
    ///     {"total_amount":null,"totalAmount":{..}} OK - a null did not set it
    ///     </code>
    ///     Python, C++ and Rust already carried these two checks; C#, Go and JavaScript did not.
    /// </summary>
    public class CelProtobufResultKeyTests
    {
        private const string SchemaText = @"
syntax = ""proto3"";
package example;
message Person {
  string favorite_color = 1;
  int32 favorite_number = 2;
  string name = 3;
  oneof pii_oneof {
    int32 oneof_int32 = 4;
    string oneof_string = 5;
  }
}";

        public CelProtobufResultKeyTests()
        {
            CelExecutor.Register();
        }

        // ProtobufResultWriter is internal, so this goes through the executor, as the other
        // write-back tests do: a message-level transform returning a map is what reaches Fill.
        private static async Task<object> Transform(string expr)
        {
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL", null, null,
                expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);
            return await new CelExecutor().Transform(ctx, new Person { Name = "alice" });
        }

        // Both orders, because order deciding the winner was the defect.
        [Theory]
        [InlineData("{'oneof_int32': 7, 'oneof_string': 's'}")]
        [InlineData("{'oneof_string': 's', 'oneof_int32': 7}")]
        public async Task TwoMembersOfOneOneofAreReported(string expr)
        {
            var ex = await Assert.ThrowsAnyAsync<RuleException>(() => Transform(expr));

            Assert.Contains("more than one member of oneof", ex.ToString());
            // Sorted, so the message does not depend on enumeration order.
            Assert.Contains("oneof_int32 and oneof_string", ex.ToString());
        }

        // A null does not occupy the oneof, so this is the reference's OK case.
        [Fact]
        public async Task ANullAlongsideAOneofMemberIsAccepted()
        {
            var result = Assert.IsType<Person>(
                await Transform("{'oneof_string': 's', 'oneof_int32': null}"));

            Assert.Equal("s", result.OneofString);
        }

        // The must-fail twin: a single member still writes.
        [Fact]
        public async Task ASingleOneofMemberStillWrites()
        {
            var result = Assert.IsType<Person>(await Transform("{'oneof_int32': 7}"));

            Assert.Equal(7, result.OneofInt32);
        }

        // FindField accepts a field's declared name and its JSON name, so these are one field.
        [Theory]
        [InlineData("{'favorite_color': 'blue', 'favoriteColor': 'red'}")]
        // Refused too: the JVM tests hasField *before* its null early-return.
        [InlineData("{'favorite_color': 'blue', 'favoriteColor': null}")]
        public async Task OneFieldNamedUnderBothSpellingsIsReported(string expr)
        {
            var ex = await Assert.ThrowsAnyAsync<RuleException>(() => Transform(expr));

            Assert.Contains("twice", ex.ToString());
            Assert.Contains("favoriteColor and favorite_color", ex.ToString());
        }

        // The reference's OK case for the same pair: the first entry set nothing.
        [Fact]
        public async Task ANullThenAValueForOneFieldIsAccepted()
        {
            var result = Assert.IsType<Person>(
                await Transform("{'favorite_color': null, 'favoriteColor': 'blue'}"));

            Assert.Equal("blue", result.FavoriteColor);
        }
    }
}
