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
using Confluent.SchemaRegistry.Encryption;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests;

public class UpdateKekTests
{
    [Fact]
    public void UpdateKekWithSameContentShouldBeEqualAndHaveSameHashCode()
    {
        var updateKek1 = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" }, { "prop2", "value2" } },
            doc: "doc1",
            shared: true
        );

        var updateKek2 = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop2", "value2" }, { "prop1", "value1" } },
            doc: "doc1",
            shared: true
        );

        Assert.True(updateKek1.Equals(updateKek2));
        Assert.True(updateKek2.Equals(updateKek1));
        Assert.Equal(updateKek1.GetHashCode(), updateKek2.GetHashCode());
    }

    [Fact]
    public void UpdateKekEqualsShouldBeFalseWhenKmsPropsDiffer()
    {
        var updateKek1 = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        var updateKek2 = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value2" } }
        );

        Assert.NotEqual(updateKek1, updateKek2);
        Assert.NotEqual(updateKek1.GetHashCode(), updateKek2.GetHashCode());
    }

    [Fact]
    public void UpdateKekEqualsShouldBeFalseWhenComparedToNull()
    {
        var updateKek = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        Assert.False(updateKek.Equals(null));
    }

    [Fact]
    public void UpdateKekEqualsShouldBeFalseWhenComparedToDifferentType()
    {
        var updateKek = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        // ReSharper disable once SuspiciousTypeConversion.Global
        Assert.False(updateKek.Equals("not an updateKek"));
    }

    [Fact]
    public void UpdateKekEqualsShouldBeTrueWhenComparedToSelf()
    {
        var updateKek = CreateUpdateKek(
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        Assert.True(updateKek.Equals(updateKek));
    }

    private static UpdateKek CreateUpdateKek(
        IDictionary<string, string> kmsProps,
        string doc = "defaultDoc",
        bool shared = false)
    {
        return new UpdateKek
        {
            KmsProps = kmsProps,
            Doc = doc,
            Shared = shared
        };
    }
}