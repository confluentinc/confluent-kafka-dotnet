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

using System.Threading.Tasks;
using Confluent.SchemaRegistry.Rules;
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

        [Fact]
        public void RegisterInstallsTheGlobalExecutor()
        {
            CelValidator.Register();
            Assert.NotNull(RuleRegistry.GlobalInstance.GetValidationExecutor());
        }
    }
}
