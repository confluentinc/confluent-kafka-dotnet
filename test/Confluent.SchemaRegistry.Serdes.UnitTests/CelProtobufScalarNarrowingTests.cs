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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Narrowing a CEL value to a scalar field's type. Every arm of the writer's
    ///     <c>Scalar</c> was a bare <c>System.Convert.To*</c>, which coerces rather than checks,
    ///     so a wrong-typed or inexact rule result was accepted and silently changed meaning:
    ///     <c>ToInt32(1.9)</c> gave <b>2</b> (half-to-even, so 2.5 also gave 2),
    ///     <c>ToBoolean(0)</c> gave false, <c>ToBoolean("TRUE")</c> gave true,
    ///     <c>ToSingle(true)</c> gave 1, <c>ToSingle(1e40)</c> gave +Infinity, and
    ///     <c>ToString()</c> wrote any value at all into a string field as its .NET text.
    ///
    ///     <para>The contract is protobuf's own JSON parser, which is what the JVM's write-back
    ///     parses the result map with. Measured against protobuf-java 4.35.1:</para>
    ///     <code>
    ///       int32  &lt;- 1.9        REJECT "Not an int32 value: 1.9"
    ///       int32  &lt;- 2.0        2
    ///       int32  &lt;- 2147483648 REJECT "Not an int32 value"
    ///       int32  &lt;- true       REJECT "Not an int32 value: true"
    ///       bool   &lt;- 0          REJECT "Invalid bool value: 0"
    ///       bytes  &lt;- 5          REJECT
    ///       float  &lt;- 1.0e40     REJECT "Out of range float value: 1.0e40"
    ///       double &lt;- 3          3.0
    ///     </code>
    ///     <para>That parser is also lenient the other way - it stringifies a number into a
    ///     string field, reads "true"/"false" as a bool and a numeric string as a number - and
    ///     none of that is followed. Those coercions exist only because its input crossed a JSON
    ///     transport, which this writer does not cross.</para>
    ///
    ///     <para>The well-known wrapper types are the fixture because each is a message with one
    ///     field of a single scalar kind, so one message per kind needs no new .proto.</para>
    /// </summary>
    public class CelProtobufScalarNarrowingTests
    {
        // The message's own descriptor drives the rebuild, so the schema text only has to parse.
        private const string SchemaText = @"syntax = ""proto3"";
package tests;
message Unused { string label = 1; }";

        private static async Task<object> Write(IMessage target, string expr)
        {
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL", null, null,
                expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);
            return await new CelExecutor().Transform(ctx, target);
        }

        /// <summary>An exact conversion is still a conversion, and still happens.</summary>
        [Fact]
        public async Task ExactConversionsAreKept()
        {
            Assert.Equal(2, Assert.IsType<Int32Value>(
                await Write(new Int32Value(), "{'value': 2.0}")).Value);
            Assert.Equal(2, Assert.IsType<Int32Value>(
                await Write(new Int32Value(), "{'value': 2}")).Value);
            // Every numeric kind a rule can compute reaches a floating field.
            Assert.Equal(3.0, Assert.IsType<DoubleValue>(
                await Write(new DoubleValue(), "{'value': 3}")).Value);
            Assert.Equal(3.0, Assert.IsType<DoubleValue>(
                await Write(new DoubleValue(), "{'value': 3u}")).Value);
            Assert.Equal(1.5f, Assert.IsType<FloatValue>(
                await Write(new FloatValue(), "{'value': 1.5}")).Value);
            Assert.Equal(ByteString.CopyFromUtf8("ab"), Assert.IsType<BytesValue>(
                await Write(new BytesValue(), "{'value': b'ab'}")).Value);
            // The whole uint64 domain round-trips: the decimal carrier holds it exactly.
            Assert.Equal(ulong.MaxValue, Assert.IsType<UInt64Value>(
                await Write(new UInt64Value(), "{'value': 18446744073709551615u}")).Value);
        }

        /// <summary>
        ///     An inexact or out-of-range number. Each of these was silently changed before:
        ///     1.9 became 2, 2147483648 raised a bare OverflowException naming no field, and
        ///     1e40 became +Infinity.
        /// </summary>
        [Fact]
        public async Task InexactOrOutOfRangeNumbersAreRefused()
        {
            await AssertRuleError(new Int32Value(), "{'value': 1.9}", "non-integral");
            // Half-to-even is what made this one worse than truncation: 2.5 rounded *down*.
            await AssertRuleError(new Int32Value(), "{'value': 2.5}", "non-integral");
            await AssertRuleError(new Int32Value(), "{'value': 2147483648}", "out of range");
            await AssertRuleError(new Int32Value(), "{'value': -2147483649}", "out of range");
            await AssertRuleError(new UInt32Value(), "{'value': -1}", "out of range");
            await AssertRuleError(new FloatValue(), "{'value': 1.0e40}", "out of range float");
            await AssertRuleError(new FloatValue(), "{'value': -1.0e40}", "out of range float");
        }

        /// <summary>
        ///     A value of the wrong kind entirely. The bool cases matter most: .NET reads 0/1 and
        ///     "TRUE" as a bool where protobuf JSON refuses all three.
        /// </summary>
        [Fact]
        public async Task AValueOfTheWrongKindIsRefused()
        {
            await AssertRuleError(new StringValue(), "{'value': 1}", "to string field");
            await AssertRuleError(new StringValue(), "{'value': true}", "to string field");
            await AssertRuleError(new BoolValue(), "{'value': 0}", "to bool field");
            await AssertRuleError(new BoolValue(), "{'value': 1}", "to bool field");
            await AssertRuleError(new BoolValue(), "{'value': 'false'}", "to bool field");
            await AssertRuleError(new BoolValue(), "{'value': 'TRUE'}", "to bool field");
            await AssertRuleError(new BytesValue(), "{'value': 5}", "to bytes field");
            await AssertRuleError(new BytesValue(), "{'value': 'YWI='}", "to bytes field");
            await AssertRuleError(new Int32Value(), "{'value': true}", "bool to integer field");
            await AssertRuleError(new DoubleValue(), "{'value': true}", "bool to float field");
        }

        /// <summary>
        ///     NaN and the infinities are not range errors: <c>JsonFormat.parseFloat</c> accepts
        ///     those explicitly, and only a *finite* value outside the float range is refused.
        /// </summary>
        [Fact]
        public async Task NonFiniteFloatsPassThrough()
        {
            Assert.True(float.IsNaN(Assert.IsType<FloatValue>(
                await Write(new FloatValue(), "{'value': double('NaN')}")).Value));
            Assert.True(float.IsPositiveInfinity(Assert.IsType<FloatValue>(
                await Write(new FloatValue(), "{'value': double('Infinity')}")).Value));
        }

        private static async Task AssertRuleError(IMessage target, string expr, string expected)
        {
            Exception e = await Assert.ThrowsAnyAsync<Exception>(() => Write(target, expr));
            Assert.Contains(expected, e.Message, StringComparison.Ordinal);
        }
    }
}
