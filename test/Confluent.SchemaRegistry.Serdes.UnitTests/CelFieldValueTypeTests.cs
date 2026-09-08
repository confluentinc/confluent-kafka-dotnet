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
using System.Numerics;
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
    ///     <c>CEL_FIELD</c> rules over protobuf decimal and timestamp fields.
    ///     and C5).
    ///
    ///     Avro carries these two as logical types on a primitive, so the field is a leaf and a
    ///     field rule reaches it. Protobuf carries them as messages, so the walk used to descend
    ///     <i>past</i> the field and transform value/scale or seconds/nanos one at a time -
    ///     meaning a rule tagged for the field never fired at all, and the message came back
    ///     unchanged with no error. A silent no-op is the worst of the three possible outcomes:
    ///     the rule author gets no signal.
    ///
    ///     This is the port of the JVM client's #4538 (<c>isCelLeafMessage</c>). Variant is
    ///     deliberately not a leaf - it is a record in Avro too, so skipping it is the behaviour
    ///     that matches, and a variant is reached with a message-level <c>CEL</c> rule instead.
    /// </summary>
    public class CelFieldValueTypeTests
    {
        /// <summary>
        ///     The same text as proto/Parity.proto, which is what Generated/Parity.cs is compiled
        ///     from. Inlined rather than read from disk so the test does not depend on the working
        ///     directory at run time, matching how the other CEL tests here carry their schema.
        /// </summary>
        private const string SchemaText = @"
syntax = ""proto3"";

package parity;

import ""confluent/meta.proto"";
import ""confluent/type/decimal.proto"";
import ""confluent/type/variant.proto"";
import ""google/protobuf/timestamp.proto"";

// C1/C2: inline rules at message and field level, all expected to PASS.
message ParityRecord {
  option (.confluent.message_meta) = {
    rules: [
      {name: ""msgDec"", expr: ""decimals.gt(this.amount, decimal('10.00'))""},
      {name: ""msgTs"",  expr: ""this.ts > timestamp('2000-01-01T00:00:00Z')""},
      {name: ""msgVar"", expr: ""variants.type(this.data) == 'object'""}
    ]
  };
  .confluent.type.Decimal amount = 1 [(.confluent.field_meta) = {
    rules: [{name: ""fldDec"", expr: ""decimals.gt(decimal(this), decimal('10.00'))""}]
  }];
  google.protobuf.Timestamp ts = 2 [(.confluent.field_meta) = {
    rules: [{name: ""fldTs"", expr: ""this > timestamp('2000-01-01T00:00:00Z')""}]
  }];
  .confluent.type.Variant data = 3 [(.confluent.field_meta) = {
    rules: [{name: ""fldVar"", expr: ""variants.type(this) == 'object'""}]
  }];
  string plain = 4;
}

// The must-fail twin: every rule inverted. A correct client reports 6 violations.
message ParityRecordN {
  option (.confluent.message_meta) = {
    rules: [
      {name: ""msgDecN"", expr: ""decimals.gt(this.amount, decimal('1000.00'))""},
      {name: ""msgTsN"",  expr: ""this.ts > timestamp('2050-01-01T00:00:00Z')""},
      {name: ""msgVarN"", expr: ""variants.type(this.data) == 'array'""}
    ]
  };
  .confluent.type.Decimal amount = 1 [(.confluent.field_meta) = {
    rules: [{name: ""fldDecN"", expr: ""decimals.gt(decimal(this), decimal('1000.00'))""}]
  }];
  google.protobuf.Timestamp ts = 2 [(.confluent.field_meta) = {
    rules: [{name: ""fldTsN"", expr: ""this > timestamp('2050-01-01T00:00:00Z')""}]
  }];
  .confluent.type.Variant data = 3 [(.confluent.field_meta) = {
    rules: [{name: ""fldVarN"", expr: ""variants.type(this) == 'array'""}]
  }];
  string plain = 4;
}

// No inline rules: used for the domain rules.
message ParityPlain {
  .confluent.type.Decimal amount = 1 [(.confluent.field_meta) = { tags: [""AMOUNT""] }];
  google.protobuf.Timestamp ts = 2 [(.confluent.field_meta) = { tags: [""TS""] }];
  .confluent.type.Variant data = 3 [(.confluent.field_meta) = { tags: [""DATA""] }];
  string plain = 4 [(.confluent.field_meta) = { tags: [""PLAIN""] }];
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

        /// <summary>Drives the client's own walker with one tagged CEL_FIELD rule.</summary>
        private static async Task<Parity.ParityPlain> Run(string expr, RuleKind kind, string tag)
        {
            var rule = new Rule("r", kind, RuleMode.Write, "CEL_FIELD",
                new HashSet<string> { tag }, null, expr, null, null, false);
            var ctx = new RuleContext(null, null, new Schema(SchemaText, SchemaType.Protobuf),
                "topic-value", "topic", null, false, RuleMode.Write, rule, 0,
                new List<Rule> { rule }, null);

            var ft = new CelFieldExecutor().NewTransform(ctx);
            object result = await ProtobufUtils.Transform(
                ctx, ProtobufUtils.Parse(SchemaText, null), Message(), ft);
            return Assert.IsType<Parity.ParityPlain>(result);
        }

        private static BigInteger Unscaled(PbDecimal d) =>
            new BigInteger(d.Value.ToByteArray(), isUnsigned: true, isBigEndian: true);

        /// <summary>
        ///     The declared type is what makes CEL_FIELD apply at all: a Record is skipped
        ///     outright, so this is the piece the whole capability turns on.
        /// </summary>
        [Fact]
        public void FieldTypesMatchTheAvroCounterpart()
        {
            var desc = Parity.ParityPlain.Descriptor;

            Assert.True(ProtobufUtils.IsCelLeafMessage(desc.FindFieldByName("amount").MessageType));
            Assert.True(ProtobufUtils.IsCelLeafMessage(desc.FindFieldByName("ts").MessageType));
            // Variant stays a record, as in Avro - not a leaf.
            Assert.False(ProtobufUtils.IsCelLeafMessage(desc.FindFieldByName("data").MessageType));
        }

        /// <summary>C4. Before the port this reported nothing because the rule never ran.</summary>
        [Fact]
        public async Task DecimalConditionFires()
        {
            await Run("decimals.gt(decimal(value), decimal('10.00'))",
                RuleKind.Condition, "AMOUNT");
        }

        /// <summary>
        ///     The must-fail twin. Without it the test above would also pass if no rule ran at
        ///     all - which is exactly how the defect hid.
        /// </summary>
        [Fact]
        public async Task DecimalConditionFailsWhenItShould()
        {
            await Assert.ThrowsAnyAsync<System.Exception>(() =>
                Run("decimals.gt(decimal(value), decimal('1000.00'))",
                    RuleKind.Condition, "AMOUNT"));
        }

        [Fact]
        public async Task TimestampConditionFires()
        {
            await Run("value > timestamp('2000-01-01T00:00:00Z')", RuleKind.Condition, "TS");
        }

        [Fact]
        public async Task TimestampConditionFailsWhenItShould()
        {
            await Assert.ThrowsAnyAsync<System.Exception>(() =>
                Run("value > timestamp('2050-01-01T00:00:00Z')", RuleKind.Condition, "TS"));
        }

        /// <summary>C5. The rule returns a BigDecimal; it has to be encoded back.</summary>
        [Fact]
        public async Task DecimalTransformIsWrittenBack()
        {
            var result = await Run("decimals.add(decimal(value), decimal('1.00'))",
                RuleKind.Transform, "AMOUNT");

            Assert.Equal(1334, (int)Unscaled(result.Amount));
            Assert.Equal(2, result.Amount.Scale);
            // Not merely the original left alone, which is what the defect looked like.
            Assert.NotEqual(ByteString.CopyFrom(Unscaled1234), result.Amount.Value);
        }

        [Fact]
        public async Task TimestampTransformIsWrittenBack()
        {
            var result = await Run("value + duration('60s')", RuleKind.Transform, "TS");

            Assert.Equal(1700000060, result.Ts.Seconds);
            Assert.Equal(123456789, result.Ts.Nanos);
        }

        /// <summary>The pass-through: the encode must invert the decode exactly.</summary>
        [Fact]
        public async Task IdentityTransformRoundTrips()
        {
            var result = await Run("value", RuleKind.Transform, "AMOUNT");

            Assert.Equal(1234, (int)Unscaled(result.Amount));
            Assert.Equal(2, result.Amount.Scale);
        }

        /// <summary>
        ///     Variant is a record in both formats, so a field rule must not reach it. The rule
        ///     below would raise if it ran, so a clean return means it was skipped.
        /// </summary>
        [Fact]
        public async Task VariantIsStillSkipped()
        {
            var result = await Run("variants.type(value) == 'not-a-type'",
                RuleKind.Condition, "DATA");

            Assert.False(result.Data.Metadata.IsEmpty);
        }
    }
}
