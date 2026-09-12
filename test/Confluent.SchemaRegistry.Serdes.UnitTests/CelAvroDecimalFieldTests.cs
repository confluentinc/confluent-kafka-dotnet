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
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A <c>CEL_FIELD</c> rule over an Avro decimal field has to reach <c>decimals.*</c> with
    ///     a bare <c>value</c>. The field's own value is an <c>AvroDecimal</c>, and the declared
    ///     type is derived from the value as it is bound, so binding it unconverted told the
    ///     checker the type was <c>Avro.AvroDecimal</c> and no <c>decimals.*</c> overload matched
    ///     - the rule failed before it ran, while <c>decimals.add(decimal(value), ...)</c> worked.
    ///     That wrapper is why the gap went unnoticed.
    ///
    ///     <para>The reference needs no wrapper: <c>CelFieldExecutor</c> binds through
    ///     <c>CelUtils.toCelValue</c>, whose <c>normalizeAvroDecimal</c> returns a
    ///     <c>CelDecimal</c>. Python, JavaScript and C++ all accept a bare <c>value</c> too -
    ///     measured - so C# was the only client requiring the wrapper.</para>
    /// </summary>
    public class CelAvroDecimalFieldTests : BaseSerializeDeserializeTests
    {
        private const string SchemaText = @"{
  ""type"": ""record"",
  ""name"": ""D1"",
  ""fields"": [
    {""name"": ""amount"",
     ""type"": {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 8, ""scale"": 2},
     ""confluent:tags"": [""AMOUNT""]},
    {""name"": ""label"", ""type"": ""string""}
  ]
}";

        public CelAvroDecimalFieldTests() : base()
        {
            CelExecutor.Register();
            CelFieldExecutor.Register();
        }

        private GenericRecord RoundTrip(string subject, string expr, RuleKind kind)
        {
            var rule = new Rule("r", kind, RuleMode.Write, "CEL_FIELD",
                new HashSet<string> { "AMOUNT" }, null, expr, null, null, false);
            var schema = new RegisteredSchema(subject + "-value", 1, 1, SchemaText,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[SchemaText] = 1;
            subjectStore[subject + "-value"] = new List<RegisteredSchema> { schema };

            var record = new GenericRecord((RecordSchema)Avro.Schema.Parse(SchemaText));
            record.Add("amount", new AvroDecimal(1234, 2));
            record.Add("label", "hi");

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestVersion = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, subject);
            return deser.DeserializeAsync(ser.SerializeAsync(record, ctx).Result, false, ctx)
                .Result;
        }

        /// <summary>
        ///     The decimal type is nameable in a rule, which needs the registry to know the name.
        /// </summary>
        /// <remarks>
        ///     The name is this client's, not any Avro schema's, so it used to fail the check with
        ///     "undeclared reference to 'confluent'" while compiling fine against a protobuf
        ///     message. Closed by registering the type, which <c>AvroRegistry</c> could not do
        ///     before Cel.NET 2.3.1. The second case is the same assertion without naming the
        ///     type and passed all along, so the pair locates the gap in the name.
        /// </remarks>
        [Theory]
        [InlineData("name-decl", "type(value) == confluent.type.Decimal")]
        [InlineData("name-same", "type(value) == type(decimal(\"1.00\"))")]
        public void TheDecimalTypeIsNameableInARule(string subject, string expr)
        {
            // A condition that holds round-trips the record unchanged.
            GenericRecord got = RoundTrip(subject, expr, RuleKind.Condition);
            Assert.Equal("12.34", ((AvroDecimal)got["amount"]).ToString());
        }

        /// <summary>A bare <c>value</c>, the form the reference and three other clients accept.</summary>
        [Theory]
        [InlineData("bare-t", "decimals.add(value, decimal(\"1.00\"))", "13.34")]
        [InlineData("bare-c", "decimals.gt(value, decimal(\"1.00\"))", "12.34")]
        public void ABareValueReachesTheDecimalFunctions(string subject, string expr, string expected)
        {
            RuleKind kind = expr.StartsWith("decimals.gt") ? RuleKind.Condition : RuleKind.Transform;
            GenericRecord got = RoundTrip(subject, expr, kind);
            Assert.Equal(expected, ((AvroDecimal)got["amount"]).ToString());
        }

        /// <summary>
        ///     The wrapped form keeps working - <c>decimal(aDecimal)</c> is idempotent - so the
        ///     fix cannot be satisfied by breaking the expression that used to be required.
        /// </summary>
        [Theory]
        [InlineData("wrap-t", "decimals.add(decimal(value), decimal(\"1.00\"))", "13.34")]
        [InlineData("wrap-c", "decimals.gt(decimal(value), decimal(\"1.00\"))", "12.34")]
        public void TheWrappedFormStillWorks(string subject, string expr, string expected)
        {
            RuleKind kind = expr.StartsWith("decimals.gt") ? RuleKind.Condition : RuleKind.Transform;
            GenericRecord got = RoundTrip(subject, expr, kind);
            Assert.Equal(expected, ((AvroDecimal)got["amount"]).ToString());
        }
    }
}
