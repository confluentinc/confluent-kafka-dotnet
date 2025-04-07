// Copyright 2024 Confluent Inc.
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
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests;

public class RuleTests
{
    [Fact]
    public void RuleWithSameContentShouldBeEqualAndHaveSameHashCode()
    {
        var rule1 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1", "tag2" },
            parameters: new Dictionary<string, string> { { "param1", "value1" }, { "param2", "value2" } },
            expr: "expr1",
            onSuccess: "onSuccess1",
            onFailure: "onFailure1",
            disabled: false
        );

        var rule2 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag2", "tag1" },
            parameters: new Dictionary<string, string> { { "param2", "value2" }, { "param1", "value1" } },
            expr: "expr1",
            onSuccess: "onSuccess1",
            onFailure: "onFailure1",
            disabled: false
        );

        Assert.True(rule1.Equals(rule2));
        Assert.True(rule2.Equals(rule1));
        Assert.Equal(rule1.GetHashCode(), rule2.GetHashCode());
    }

    [Fact]
    public void RuleEqualsShouldBeFalseWhenTagsDiffer()
    {
        var rule1 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1", "tag2" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        var rule2 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1", "tag3" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        Assert.NotEqual(rule1, rule2);
        Assert.NotEqual(rule1.GetHashCode(), rule2.GetHashCode());
    }

    [Fact]
    public void RuleEqualsShouldBeFalseWhenParametersDiffer()
    {
        var rule1 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        var rule2 = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1" },
            parameters: new Dictionary<string, string> { { "param1", "value2" } }
        );

        Assert.NotEqual(rule1, rule2);
        Assert.NotEqual(rule1.GetHashCode(), rule2.GetHashCode());
    }

    [Fact]
    public void RuleEqualsShouldBeFalseWhenComparedToNull()
    {
        var rule = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        Assert.False(rule.Equals(null));
    }

    [Fact]
    public void RuleEqualsShouldBeFalseWhenComparedToDifferentType()
    {
        var rule = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        // ReSharper disable once SuspiciousTypeConversion.Global
        Assert.False(rule.Equals("not a rule"));
    }

    [Fact]
    public void RuleEqualsShouldBeTrueWhenComparedToSelf()
    {
        var rule = CreateRule(
            name: "rule1",
            kind: RuleKind.Condition,
            mode: RuleMode.UpDown,
            type: "type1",
            tags: new HashSet<string> { "tag1" },
            parameters: new Dictionary<string, string> { { "param1", "value1" } }
        );

        Assert.True(rule.Equals(rule));
    }

    private static Rule CreateRule(
        string name,
        RuleKind kind,
        RuleMode mode,
        string type,
        ISet<string> tags,
        IDictionary<string, string> parameters,
        string expr = null,
        string onSuccess = null,
        string onFailure = null,
        bool disabled = false)
    {
        return new Rule(name, kind, mode, type, tags, parameters, expr, onSuccess, onFailure, disabled);
    }
}