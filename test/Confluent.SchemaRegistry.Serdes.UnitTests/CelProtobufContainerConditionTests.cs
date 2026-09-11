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

using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf;
using Xunit;
using PbDecimal = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A <c>CEL_FIELD</c> condition on a **repeated** field.
    ///
    ///     The rule is evaluated once per element and the verdicts are then dropped: the
    ///     reference collects them into an untyped list, and the field-level check that raises
    ///     tests for <c>false</c>, which a list never is. A condition therefore does not apply
    ///     to a container field - the intended contract.
    ///
    ///     The walk here collected the verdicts into a <c>List&lt;T&gt;</c> built for the element
    ///     type instead, so adding a <c>bool</c> threw <c>ArgumentException</c> and *both* twins
    ///     failed - the cell errored whatever the rule answered.
    /// </summary>
    public class CelProtobufContainerConditionTests
    {
        private const string SchemaText = @"
syntax = ""proto3"";
package parity;

import ""confluent/meta.proto"";
import ""confluent/type/decimal.proto"";

message ValueTypeContainers {
  repeated .confluent.type.Decimal amounts = 1 [(.confluent.field_meta) = {
    tags: [""AMOUNTS""]
  }];
  map<string, .confluent.type.Decimal> amount_map = 2 [(.confluent.field_meta) = {
    tags: [""AMOUNTMAP""]
  }];
  ValueTypeNested nested = 3;
  string label = 4;
}

message ValueTypeNested {
  .confluent.type.Decimal inner = 1 [(.confluent.field_meta) = { tags: [""INNER""] }];
}";

        public CelProtobufContainerConditionTests()
        {
            CelExecutor.Register();
            CelFieldExecutor.Register();
        }

        /// <summary>
        ///     confluent.type.Decimal.value is a *signed* big-endian unscaled integer, so it is
        ///     encoded and read back signed. Writing the magnitude instead turns 2.22 into -0.34
        ///     and an unsigned reader shows it as 2.22 either way.
        /// </summary>
        private static PbDecimal Dec(string text)
        {
            var unscaled = new BigInteger(decimal.Parse(text) * 100m);
            return new PbDecimal
            {
                Value = ByteString.CopyFrom(unscaled.ToByteArray(isUnsigned: false,
                    isBigEndian: true)),
                Precision = 8,
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
            m.Amounts.Add(Dec("2.22"));
            m.AmountMap["a"] = Dec("3.33");
            m.Nested = new Parity.ValueTypeNested { Inner = Dec("4.44") };
            return m;
        }

        private static async Task<Parity.ValueTypeContainers> Run(string expr, RuleKind kind, string tag)
        {
            var rule = new Rule("r", kind, RuleMode.Write, "CEL_FIELD",
                new HashSet<string> { tag }, null, expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);

            var ft = new CelFieldExecutor().NewTransform(ctx);
            object result = await ProtobufUtils.Transform(
                ctx, ProtobufUtils.Parse(SchemaText, null), Message(), ft);
            return Assert.IsType<Parity.ValueTypeContainers>(result);
        }

        [Fact]
        public async Task ARepeatedConditionThatHoldsLeavesTheArrayAlone()
        {
            var result = await Run("decimals.gt(decimal(value), decimal('1.00'))",
                RuleKind.Condition, "AMOUNTS");

            Assert.Equal(new[] { "1.11", "2.22" }, result.Amounts.Select(Show));
        }

        /// <summary>
        ///     The twin: the verdict is false for both elements and
        ///     the walk still has to pass, because a condition does not apply to a container
        ///     field. Before the fix this threw ArgumentException, exactly as the twin above did.
        /// </summary>
        [Fact]
        public async Task ARepeatedConditionThatFailsIsDroppedRatherThanRaised()
        {
            var result = await Run("decimals.gt(decimal(value), decimal('100.00'))",
                RuleKind.Condition, "AMOUNTS");

            Assert.Equal(new[] { "1.11", "2.22" }, result.Amounts.Select(Show));
        }

        /// <summary>
        ///     The discriminator for the two above: dropping the verdict must not become
        ///     skipping the field. A rule that cannot evaluate on a decimal has to surface,
        ///     which it can only do if every element was handed to it.
        /// </summary>
        [Fact]
        public async Task ARepeatedConditionIsStillEvaluatedPerElement()
        {
            await Assert.ThrowsAnyAsync<System.Exception>(() =>
                Run("variants.type(value) == 'object'", RuleKind.Condition, "AMOUNTS"));
        }

        /// <summary>
        ///     And a transform over the same field still writes every element - the verdict is
        ///     what is dropped, not the walk.
        /// </summary>
        [Fact]
        public async Task ARepeatedTransformStillWritesEveryElement()
        {
            var result = await Run("decimals.add(decimal(value), decimal('1.00'))",
                RuleKind.Transform, "AMOUNTS");

            Assert.Equal(new[] { "2.11", "3.22" }, result.Amounts.Select(Show));
        }
    }
}
