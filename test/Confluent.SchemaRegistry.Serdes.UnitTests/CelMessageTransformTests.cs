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
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Xunit;
using PbDecimal = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;
using PbVariant = Confluent.SchemaRegistry.Serdes.Protobuf.Variant;
using SrVariant = Confluent.SchemaRegistry.Variant;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Message-level <c>CEL</c> transforms over protobuf: the rule returns a map and the
    ///     message is rebuilt from it.
    ///
    ///     Before this the result stayed a plain dictionary, which the protobuf serializer cannot
    ///     write, so every message-level transform failed - including an identity one.
    ///
    ///     The transform has <b>replace</b> semantics: the map is the new message, so a field the
    ///     rule does not name is dropped and a <c>null</c> clears its field. Both are covered
    ///     here, because they are the part a rule author is most likely to be surprised by.
    ///
    ///     These run the real executor and then the real writer. The wiring between them is
    ///     <c>ShapeTransformResult</c>, overridden by the protobuf serde - C# cannot place the
    ///     conversion inside the CEL executor the way the other clients do, because
    ///     <c>Confluent.SchemaRegistry.Rules</c> does not reference the protobuf assembly.
    /// </summary>
    public class CelMessageTransformTests
    {
        private const string SchemaText = @"
syntax = ""proto3"";
package tests;
import ""confluent/type/decimal.proto"";
import ""confluent/type/variant.proto"";
import ""google/protobuf/timestamp.proto"";
message ValueTypes {
  confluent.type.Decimal amount = 1;
  google.protobuf.Timestamp ts = 2;
  confluent.type.Variant data = 3;
  string label = 4;
  int32 count = 5;
}";

        // 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
        private static readonly byte[] Unscaled1234 = { 0x04, 0xD2 };

        private static Parity.ParityPlain Message()
        {
            var v = SrVariant.ParseJson("{\"name\":\"alice\"}");
            return new Parity.ParityPlain
            {
                Amount = new PbDecimal
                {
                    Value = ByteString.CopyFrom(Unscaled1234), Precision = 8, Scale = 2
                },
                Ts = new Google.Protobuf.WellKnownTypes.Timestamp
                {
                    Seconds = 1700000000, Nanos = 123456789
                },
                Data = new PbVariant
                {
                    Metadata = ByteString.CopyFrom(v.MetadataBytes),
                    Value = ByteString.CopyFrom(v.ValueBytes)
                },
                Plain = "hi"
            };
        }

        /// <summary>Runs the rule, then the write-back, exactly as the serde does.</summary>
        private static async Task<Parity.ParityPlain> Transform(string expr, IMessage msg = null)
        {
            msg ??= Message();
            var rule = new Rule("r", RuleKind.Transform, RuleMode.Write, "CEL", null, null,
                expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);

            // Transform does the rebuild itself, as it does in every other client - so a result
            // that is still a map, or still a bare dictionary, fails right here.
            object result = await new CelExecutor().Transform(ctx, msg);
            return Assert.IsType<Parity.ParityPlain>(result);
        }

        private static System.Numerics.BigInteger Unscaled(PbDecimal d) =>
            new System.Numerics.BigInteger(d.Value.ToByteArray(), isUnsigned: true,
                isBigEndian: true);

        private const string AllFields =
            "'amount': message.amount, 'ts': message.ts, " +
            "'data': message.data, 'plain': message.plain";

        /// <summary>
        ///     An identity transform is the cheapest regression test for a write-back path: it
        ///     fails for any breakage in the plumbing, without depending on the computation.
        /// </summary>
        [Fact]
        public async Task PassThroughReturnsAMessageUnchanged()
        {
            var result = await Transform("{" + AllFields + "}");

            Assert.Equal(1234, (int)Unscaled(result.Amount));
            Assert.Equal(2, result.Amount.Scale);
            Assert.Equal(1700000000, result.Ts.Seconds);
            Assert.Equal(123456789, result.Ts.Nanos);
            Assert.Equal("{\"name\":\"alice\"}",
                new SrVariant(result.Data.Value.ToByteArray(),
                    result.Data.Metadata.ToByteArray()).ToJson());
            Assert.Equal("hi", result.Plain);
        }

        [Fact]
        public async Task ComputedDecimalIsWrittenBack()
        {
            var result = await Transform(
                "{'amount': decimals.add(decimal(message.amount), decimal('1.00')), " +
                "'ts': message.ts, 'data': message.data, 'plain': message.plain}");

            Assert.Equal(1334, (int)Unscaled(result.Amount));
            Assert.Equal(2, result.Amount.Scale);
        }

        /// <summary>
        ///     Precision is the unscaled value's digit count, which is what
        ///     <c>BigDecimal.precision()</c> reports and what the JVM's ProtobufResultWriter
        ///     writes (<c>m.put("precision", dec.precision())</c>). It was left unset here, so the
        ///     same computed decimal serialized differently than it does on the JVM.
        /// </summary>
        [Fact]
        public async Task ComputedDecimalCarriesItsPrecision()
        {
            var result = await Transform(
                "{'amount': decimals.add(decimal(message.amount), decimal('1.00')), " +
                "'ts': message.ts, 'data': message.data, 'plain': message.plain}");

            // 12.34 + 1.00 = 13.34 -> unscaled 1334, four digits.
            Assert.Equal(1334, (int)Unscaled(result.Amount));
            Assert.Equal(2, result.Amount.Scale);
            Assert.Equal(4u, result.Amount.Precision);
        }

        [Fact]
        public async Task ComputedTimestampIsWrittenBack()
        {
            var result = await Transform(
                "{'amount': message.amount, 'ts': message.ts + duration('60s'), " +
                "'data': message.data, 'plain': message.plain}");

            Assert.Equal(1700000060, result.Ts.Seconds);
            Assert.Equal(123456789, result.Ts.Nanos);
        }

        /// <summary>
        ///     Asserted through the decoded JSON rather than the metadata bytes: metadata holds
        ///     the field names, so the two documents share it and comparing metadata would prove
        ///     nothing.
        /// </summary>
        [Fact]
        public async Task ComputedVariantIsWrittenBack()
        {
            var result = await Transform(
                "{'amount': message.amount, 'ts': message.ts, " +
                "'data': variants.parseJson('{\"name\":\"bob\"}'), 'plain': message.plain}");

            Assert.Equal("{\"name\":\"bob\"}",
                new SrVariant(result.Data.Value.ToByteArray(),
                    result.Data.Metadata.ToByteArray()).ToJson());
        }

        /// <summary>
        ///     Replace semantics, and the consequence most likely to surprise: a rule naming only
        ///     the field it changes discards everything else. Intended, but silent on protobuf -
        ///     proto3 has no required fields, so nothing catches it.
        /// </summary>
        [Fact]
        public async Task AFieldTheRuleDoesNotNameIsDropped()
        {
            var result = await Transform("{'plain': 'changed'}");

            Assert.Equal("changed", result.Plain);
            Assert.Null(result.Amount);
            Assert.Null(result.Ts);
            Assert.Null(result.Data);
        }

        /// <summary>
        ///     The idiom for preserving absence across a transform that echoes a field is
        ///     <c>has(x) ? x : null</c>; without a null arm there would be no way to express it.
        /// </summary>
        [Fact]
        public async Task NullClearsAField()
        {
            var result = await Transform(
                "{'amount': null, 'ts': message.ts, 'data': message.data, " +
                "'plain': message.plain}");

            Assert.Null(result.Amount);
            Assert.NotNull(result.Ts);
            Assert.Equal("hi", result.Plain);
        }

        /// <summary>
        ///     <c>has(x) ? x : null</c> preserves absence across a transform that echoes a field.
        ///     This is the portable idiom and it holds in every client.
        ///
        ///     The <i>unguarded</i> echo is deliberately not asserted here, because it depends on
        ///     the Cel.NET version: 2.2.0 binds an unset message as CEL null (so the echo leaves
        ///     the field absent), while the unreleased 2.2.1 binds it as the default instance (so
        ///     the echo materialises it, as Java, Python and Go do). Pinning either would make
        ///     this test a version detector rather than a contract test.
        /// </summary>
        [Fact]
        public async Task GuardPreservesAbsence()
        {
            var absent = new Parity.ParityPlain { Plain = "hi" };

            var guarded = await Transform(
                "{'amount': has(message.amount) ? message.amount : null, " +
                "'plain': message.plain}", absent);

            Assert.Null(guarded.Amount);
            Assert.Equal("hi", guarded.Plain);
        }

        /// <summary>
        ///     Sub-second precision survives the round trip.
        ///
        ///     It did not before: Cel.NET bound a protobuf Timestamp by calling
        ///     <c>Instant.PlusNanoseconds</c> and discarding the result - Instant is immutable -
        ///     so 1700000000.123 reached a rule as 1700000000.000 and an identity transform could
        ///     not restore what it never saw. Fixed in Cel.NET (PbTypeDescription.AsTimestamp);
        ///     its own MaybeUnwrap case used nanos = 0 and could never have caught it.
        /// </summary>
        [Fact]
        public async Task TimestampSubSecondPrecisionSurvives()
        {
            var result = await Transform("{" + AllFields + "}");

            Assert.Equal(1700000000, result.Ts.Seconds);
            Assert.Equal(123456789, result.Ts.Nanos);
        }

        /// <summary>A CONDITION answers with a bool, which must never reach the rebuild.</summary>
        [Fact]
        public async Task ConditionResultsAreLeftAlone()
        {
            var rule = new Rule("r", RuleKind.Condition, RuleMode.Write, "CEL", null, null,
                "decimals.gt(message.amount, decimal('10.00'))", null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);

            object result = await new CelExecutor().Transform(ctx, Message());

            // Transform must hand a CONDITION's bool straight back: it is a pass/fail signal to
            // the serde, not data to rebuild a message from. Rebuilding it would throw, and
            // returning anything but a bool would make the serde read the condition as a failure.
            Assert.True(Assert.IsType<bool>(result));
        }
    }
}
