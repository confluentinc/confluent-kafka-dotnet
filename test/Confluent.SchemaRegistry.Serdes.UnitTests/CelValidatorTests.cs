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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
using Google.Protobuf.Collections;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Tests for CelValidator — the per-rule CEL semantics, independent of any walker.
    /// </summary>
    public class CelValidatorTests
    {
        private static ValidationRule Rule(string expr, string name = "r", string doc = null) =>
            new ValidationRule { Name = name, Expr = expr, Doc = doc };

        public class Person
        {
            public int Age { get; set; }
            public string Name { get; set; }
        }

        [Theory]
        [InlineData("this >= 0", 30, true)]
        [InlineData("this >= 0", -5, false)]
        [InlineData("size(this) > 0", "alice", true)]
        [InlineData("size(this) > 0", "", false)]
        [InlineData("this.startsWith('a')", "alice", true)]
        [InlineData("this in ['a', 'b']", "a", true)]
        public async Task BooleanRules(string expr, object value, bool expected)
        {
            var validator = new CelValidator();
            var result = await validator.Execute(Rule(expr), null, value);
            Assert.Equal(expected, result);
        }

        [Fact]
        public async Task ObjectFieldAccess()
        {
            var validator = new CelValidator();
            var person = new Person { Age = 30, Name = "Alice" };
            Assert.Equal(true, await validator.Execute(Rule("this.Age <= 150"), null, person));
            Assert.Equal(false, await validator.Execute(
                Rule("this.Age <= 150"), null, new Person { Age = 200, Name = "Alice" }));
        }

        [Fact]
        public async Task StringResultIsTheFailureMessage()
        {
            var validator = new CelValidator();
            var rule = Rule("this >= 0 ? '' : 'age must be positive, got ' + string(this)");
            // An empty string means the rule passed.
            Assert.Equal("", await validator.Execute(rule, null, 5));
            Assert.Equal("age must be positive, got -5", await validator.Execute(rule, null, -5));
        }

        [Fact]
        public async Task NowIsBound()
        {
            var validator = new CelValidator();
            var result = await validator.Execute(
                Rule("now > timestamp('2000-01-01T00:00:00Z')"), null, 1);
            Assert.Equal(true, result);
        }

        [Fact]
        public async Task NullValueIsAContractViolation()
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAsync<RuleException>(
                () => validator.Execute(Rule("this > 0"), null, null));
            Assert.Contains("received a null value", ex.Message);
        }

        [Fact]
        public async Task MissingExpression()
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAsync<RuleException>(
                () => validator.Execute(new ValidationRule { Name = "r" }, null, 1));
            Assert.Contains("has no expression", ex.Message);
        }

        [Fact]
        public async Task UnnamedRuleIsReportedAsUnnamed()
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAsync<RuleException>(
                () => validator.Execute(new ValidationRule(), null, 1));
            Assert.Contains("'unnamed'", ex.Message);
        }

        [Fact]
        public async Task UncompilableExpression()
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAsync<RuleException>(
                () => validator.Execute(Rule("this >= "), null, 1));
            Assert.Contains("Could not compile validation rule 'r'", ex.Message);
        }

        [Fact]
        public async Task NonBooleanNonStringResultIsRejected()
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAsync<RuleException>(
                () => validator.Execute(Rule("1 + 1"), null, 1));
            Assert.Contains("must return bool or string", ex.Message);
        }

        // A field-level rule on a repeated or map field binds the collection itself to
        // `this`. Protobuf surfaces those as RepeatedField<T> and MapField<K,V>, which are
        // neither Dictionary<,> nor List<>, and whose elements are messages - so both the
        // declared type and the registry have to be derived from what the collection holds.
        [Fact]
        public async Task ProtobufCollectionFieldValues()
        {
            var validator = new CelValidator();
            var person = new Example.ValidationPerson { Name = "Alice", FavoriteNumber = 5 };

            Assert.Equal(true, await validator.Execute(
                Rule("this.name == 'Alice'"), null, person));
            Assert.Equal(true, await validator.Execute(
                Rule("this[0].name == 'Alice'"), null,
                new RepeatedField<Example.ValidationPerson> { person }));
            Assert.Equal(true, await validator.Execute(
                Rule("this['a'].name == 'Alice'"), null,
                new MapField<string, Example.ValidationPerson> { { "a", person } }));
            Assert.Equal(true, await validator.Execute(
                Rule("size(this) > 0"), null, new RepeatedField<string> { "t" }));
            Assert.Equal(true, await validator.Execute(
                Rule("this['a'] >= 0"), null, new MapField<string, int> { { "a", 1 } }));
        }

        [Fact]
        public void RegisterInstallsTheGlobalExecutor()
        {
            CelValidator.Register();
            Assert.NotNull(RuleRegistry.GlobalInstance.GetValidationExecutor());
        }

        /// <summary>
        ///     A rule on an unsigned field may compare it against a plain integer literal,
        ///     because BuiltinLibrary enables cross-type numeric comparisons. Without them
        ///     `this > 0` on a ulong would not type-check, and .NET would reject expressions
        ///     the Java, Go and Python clients accept.
        /// </summary>
        [Theory]
        [InlineData("this > 0", (ulong)25, true)]
        [InlineData("this > 0u", (ulong)25, true)]
        // 2^64-5 is positive unsigned; read as a signed long it would be -5.
        [InlineData("this > 0", ulong.MaxValue - 4, true)]
        [InlineData("this % 10u == 5u", (ulong)25, true)]
        [InlineData("this % 10u == 5u", ulong.MaxValue - 4, false)]
        [InlineData("this + 1u > 0u", (ulong)25, true)]
        public async Task UnsignedFieldsCompareAgainstPlainLiterals(string expr, ulong value, bool expected)
        {
            var validator = new CelValidator();
            Assert.Equal(expected, await validator.Execute(Rule(expr), null, value));
        }

        /// <summary>
        ///     Ordering is all that widens. Equality and arithmetic against a plain integer
        ///     literal still fail to check, which is what Java, Go and Python do too - so a
        ///     rule that works in one client works in all of them.
        /// </summary>
        [Theory]
        [InlineData("this == 25")]
        [InlineData("this != 25")]
        [InlineData("this % 10 == 5")]
        public async Task UnsignedEqualityAndArithmeticStayHomogeneous(string expr)
        {
            var validator = new CelValidator();
            var ex = await Assert.ThrowsAnyAsync<Exception>(
                () => validator.Execute(Rule(expr), null, (ulong)25));
            Assert.Contains("no matching overload", ex.ToString());
        }

        // A protobuf enum reaches the executor as the generated CLR enum, which CEL has no
        // type for - Cel.NET rejects it outright with "enum not allowed here", so a rule on
        // an enum field used to fail to compile and reject every message. It is compared by
        // its number instead, as in the Java, Go and C++ clients.
        [Theory]
        [InlineData("this == 1", true)]
        [InlineData("this == 0", false)]
        [InlineData("this > 0", true)]
        public async Task EnumValuesBindByTheirNumber(string expr, bool expected)
        {
            var validator = new CelValidator();
            Assert.Equal(expected, await validator.Execute(
                Rule(expr), null, Example.ValidationColor.Green));
        }

        // A repeated enum field binds as the whole collection, so a rule about its elements
        // is a comprehension - and the elements have to be numbers there too.
        [Fact]
        public async Task RepeatedEnumValuesBindByTheirNumber()
        {
            var validator = new CelValidator();
            var greens = new List<Example.ValidationColor>
                { Example.ValidationColor.Green, Example.ValidationColor.Green };
            Assert.Equal(true, await validator.Execute(Rule("this.all(v, v == 1)"), null, greens));

            var mixed = new List<Example.ValidationColor>
                { Example.ValidationColor.Green, Example.ValidationColor.Red };
            Assert.Equal(false, await validator.Execute(Rule("this.all(v, v == 1)"), null, mixed));
        }

        // Only enums are rewritten. A byte[] is an IList of bytes and a string is a
        // collection of chars; neither may be turned into a list of numbers.
        [Fact]
        public async Task NonEnumValuesAreUnchanged()
        {
            var validator = new CelValidator();
            Assert.Equal(true, await validator.Execute(Rule("this == b'ab'"), null, new byte[] { 97, 98 }));
            Assert.Equal(true, await validator.Execute(Rule("size(this) == 2"), null, "ab"));
            Assert.Equal(true, await validator.Execute(
                Rule("this.all(v, size(v) > 0)"), null, new List<string> { "a", "b" }));
        }


        // The CEL type of a field comes from its descriptor, not from the CLR type of the
        // value. C#'s generated types imply the right type for each protobuf scalar, so the
        // two usually agree - but only by coincidence of the type system, and not at all for
        // an enum. These pin the descriptor as the authority.
        [Theory]
        // A uint64 is a CEL uint: it compares against an unsigned literal, and homogeneous
        // equality rejects a signed one - as in the Java, Go and Python clients.
        [InlineData("big", "this == 1u", true)]
        [InlineData("big", "this == 2u", false)]
        // An int64 is the mirror image.
        [InlineData("small", "this == 1", true)]
        // An enum is compared by its number.
        [InlineData("color", "this == 1", true)]
        // A repeated uint64 binds the whole list, whose elements are uints too.
        [InlineData("bigs", "this.all(v, v == 1u)", true)]
        public async Task FieldTypeComesFromTheDescriptor(string fieldName, string expr, bool expected)
        {
            var validator = new CelValidator();
            var fd = Example.ValidationTypedFields.Descriptor.FindFieldByName(fieldName);
            object value = fieldName == "bigs" ? (object)new List<ulong> { 1UL } : (object)1UL;
            if (fieldName == "small")
            {
                value = 1L;
            }

            Assert.Equal(expected, await validator.Execute(Rule(expr), fd, value));
        }

        [Theory]
        // An unsigned field does not accept a signed literal, and vice versa. Both would
        // compile if the type were inferred from a CLR long.
        [InlineData("big", "this == 1")]
        [InlineData("small", "this == 1u")]
        [InlineData("color", "this == 1u")]
        public async Task FieldTypeRejectsTheWrongLiteral(string fieldName, string expr)
        {
            var validator = new CelValidator();
            var fd = Example.ValidationTypedFields.Descriptor.FindFieldByName(fieldName);
            await Assert.ThrowsAsync<RuleException>(() => validator.Execute(Rule(expr), fd, 1L));
        }

        // The declared type and the bound value have to move together. Handing a signed CLR
        // value for an unsigned field must still evaluate as unsigned rather than fail at
        // evaluation with a type mismatch - the value is presented as the field's type too.
        [Fact]
        public async Task ValueIsPresentedAsTheDeclaredType()
        {
            var validator = new CelValidator();
            var fd = Example.ValidationTypedFields.Descriptor.FindFieldByName("big");
            // A CLR long, not a ulong, for a uint64 field.
            Assert.Equal(true, await validator.Execute(Rule("this == 1u"), fd, 1L));
            // And the bits are reinterpreted, not rejected: -1 is uint64 max.
            Assert.Equal(true, await validator.Execute(Rule("this > 0u"), fd, -1L));
        }

        // The math extension is registered in CelExecutor.WithLibraries, matching the JVM
        // client's CelExtensions.math(). This exercises it end-to-end through the validator.
        [Theory]
        [InlineData("math.greatest(1, 5, 3) == 5")]
        [InlineData("math.least([4, 2, 8]) == 2")]
        [InlineData("math.greatest(1, 2u, 3.0) == 3.0")]
        [InlineData("math.abs(-4) == 4")]
        [InlineData("math.round(2.5) == 3.0")]
        [InlineData("math.sign(-3) == -1")]
        [InlineData("math.isNaN(0.0/0.0)")]
        [InlineData("math.bitAnd(12, 10) == 8")]
        [InlineData("math.bitShiftLeft(1, 3) == 8")]
        public async Task MathExtension(string expr)
        {
            var validator = new CelValidator();
            Assert.Equal(true, await validator.Execute(Rule(expr), null, 0));
        }
    }
}
