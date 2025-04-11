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

public class KekTests
{
    [Fact]
    public void KekWithSameContentShouldBeEqualAndHaveSameHashCode()
    {
        var kek1 = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" }, { "prop2", "value2" } },
            doc: "doc1",
            shared: true,
            deleted: false
        );

        var kek2 = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop2", "value2" }, { "prop1", "value1" } },
            doc: "doc1",
            shared: true,
            deleted: false
        );

        Assert.True(kek1.Equals(kek2));
        Assert.True(kek2.Equals(kek1));
        Assert.Equal(kek1.GetHashCode(), kek2.GetHashCode());
    }

    [Fact]
    public void KekEqualsShouldBeFalseWhenKmsPropsDiffer()
    {
        var kek1 = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        var kek2 = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value2" } }
        );

        Assert.NotEqual(kek1, kek2);
        Assert.NotEqual(kek1.GetHashCode(), kek2.GetHashCode());
    }

    [Fact]
    public void KekEqualsShouldBeFalseWhenComparedToNull()
    {
        var kek = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        Assert.False(kek.Equals(null));
    }

    [Fact]
    public void KekEqualsShouldBeFalseWhenComparedToDifferentType()
    {
        var kek = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        // ReSharper disable once SuspiciousTypeConversion.Global
        Assert.False(kek.Equals("not a kek"));
    }

    [Fact]
    public void KekEqualsShouldBeTrueWhenComparedToSelf()
    {
        var kek = CreateKek(
            name: "kek1",
            kmsType: "type1",
            kmsKeyId: "key1",
            kmsProps: new Dictionary<string, string> { { "prop1", "value1" } }
        );

        Assert.True(kek.Equals(kek));
    }

    private static Kek CreateKek(
        string name,
        string kmsType,
        string kmsKeyId,
        IDictionary<string, string> kmsProps,
        string doc = "defaultDoc",
        bool shared = false,
        bool deleted = false)
    {
        return new Kek
        {
            Name = name,
            KmsType = kmsType,
            KmsKeyId = kmsKeyId,
            KmsProps = kmsProps,
            Doc = doc,
            Shared = shared,
            Deleted = deleted
        };
    }
}