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
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf;
using Xunit;
using PbDecimal = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A message-level transform answering with the wrong *shape* for a repeated or map
    ///     field used to be ignored: <c>SetMap</c>/<c>SetRepeated</c> returned early on a
    ///     mismatch. Because the writer rebuilds the message field by field, that left the
    ///     field <b>empty</b> - the rule's data silently discarded, which is the one outcome
    ///     the rest of the writer avoids (every scalar arm throws <c>Mismatch</c>).
    ///
    ///     The reference for this path is protobuf JSON: the JVM renders the CEL result map
    ///     with Jackson and parses it with <c>ProtobufSchema.fromJson</c>. Measured against
    ///     protobuf-java 4.34.0's <c>JsonFormat.parser()</c>:
    ///       {"m": "notamap"}  -> Expect a map object but found: "notamap"
    ///       {"m": [1,2]}      -> Expect a map object but found: [1,2]
    ///       {"r": "notalist"} -> Expected an array for r but found "notalist"
    ///       {"r": {"a":1}}    -> Expected an array for r but found {"a":1}
    /// </summary>
    public class CelProtobufContainerWriteBackTests
    {
        private const string SchemaText = @"
syntax = ""proto3"";
package parity;

import ""confluent/meta.proto"";
import ""confluent/type/decimal.proto"";

message ValueTypeContainers {
  repeated .confluent.type.Decimal amounts = 1;
  map<string, .confluent.type.Decimal> amount_map = 2;
  ValueTypeNested nested = 3;
  string label = 4;
}

message ValueTypeNested {
  .confluent.type.Decimal inner = 1;
}";

        public CelProtobufContainerWriteBackTests()
        {
            CelExecutor.Register();
        }

        private static PbDecimal Dec(string text)
        {
            var unscaled = new BigInteger(decimal.Parse(text) * 100m);
            return new PbDecimal
            {
                Value = ByteString.CopyFrom(unscaled.ToByteArray(isUnsigned: false,
                    isBigEndian: true)),
                Precision = 4,
                Scale = 2
            };
        }

        private static string Show(PbDecimal d) =>
            ((decimal)new BigInteger(d.Value.ToByteArray(), isUnsigned: false, isBigEndian: true)
                / 100m).ToString("0.00");

        private static Parity.ValueTypeContainers Message()
        {
            var m = new Parity.ValueTypeContainers { Label = "hi" };
            m.Amounts.Add(Dec("1.11"));
            m.AmountMap["a"] = Dec("3.33");
            return m;
        }

        private static async Task<object> Transform(string expr)
        {
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL", null, null,
                expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);
            return await new CelExecutor().Transform(ctx, Message());
        }

        [Theory]
        [InlineData("{'amount_map': 'notamap', 'label': message.label}")]
        [InlineData("{'amount_map': [1, 2], 'label': message.label}")]
        [InlineData("{'amount_map': 7, 'label': message.label}")]
        public async Task AWrongShapeForAMapFieldIsReported(string expr)
        {
            await Assert.ThrowsAnyAsync<RuleException>(() => Transform(expr));
        }

        [Theory]
        [InlineData("{'amounts': 'notalist', 'label': message.label}")]
        [InlineData("{'amounts': {'a': 1}, 'label': message.label}")]
        [InlineData("{'amounts': 7, 'label': message.label}")]
        public async Task AWrongShapeForARepeatedFieldIsReported(string expr)
        {
            // A string is especially worth pinning: it *is* IEnumerable in .NET, so without
            // the explicit test it would be spread into one element per character.
            await Assert.ThrowsAnyAsync<RuleException>(() => Transform(expr));
        }

        // The must-fail twins: the right shapes still round-trip, so the check has not simply
        // broken container write-back.
        [Fact]
        public async Task TheRightShapesStillWriteBack()
        {
            object echoed = await Transform(
                "{'amounts': message.amounts, 'amount_map': message.amount_map, " +
                "'label': message.label}");
            var result = Assert.IsType<Parity.ValueTypeContainers>(echoed);

            Assert.Equal(new[] { "1.11" }, result.Amounts.Select(Show));
            Assert.Equal("3.33", Show(result.AmountMap["a"]));
            Assert.Equal("hi", result.Label);
        }

        // A null *inside* a container was skipped, which changed the list's length (or dropped
        // a map entry) and still reported success. protobuf has no null to store, and the
        // reference refuses the document - measured against protobuf-java's JsonFormat:
        //   {"r": [null]}       -> Repeated field elements cannot be null in field: r
        //   {"m": {"a": null}}  -> Map value cannot be null.
        [Theory]
        [InlineData("{'amounts': [null], 'label': message.label}", "repeated field")]
        [InlineData("{'amounts': [message.amounts[0], null], 'label': message.label}",
            "repeated field")]
        [InlineData("{'amount_map': {'a': null}, 'label': message.label}", "map field")]
        public async Task ANullInsideAContainerIsReported(string expr, string named)
        {
            var ex = await Assert.ThrowsAnyAsync<RuleException>(() => Transform(expr));
            Assert.Contains(named, ex.ToString());
        }

        [Fact]
        public async Task AnEmptyContainerIsStillAValidShape()
        {
            // An empty list or map is a *shape match* with nothing in it, and must not be
            // confused with the mismatch above - clearing a container is a legitimate result.
            object cleared = await Transform("{'amounts': [], 'amount_map': {}, 'label': 'x'}");
            var result = Assert.IsType<Parity.ValueTypeContainers>(cleared);

            Assert.Empty(result.Amounts);
            Assert.Empty(result.AmountMap);
            Assert.Equal("x", result.Label);
        }
    }
}
