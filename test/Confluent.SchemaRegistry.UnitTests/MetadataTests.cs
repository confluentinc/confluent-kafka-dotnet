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

public class MetadataTests
{
    [Fact]
    public void MetadataWithSameContentShouldBeEqualAndHaveSameHashCode()
    {
        var metadata1 = CreateMetadata(
            tags: new Dictionary<string, ISet<string>>
            {
                { "key1", new HashSet<string> { "value1", "value2" } },
                { "key2", new HashSet<string> { "value3", "value4" } }
            },
            properties: new Dictionary<string, string>
            {
                { "key1", "value1" },
                { "key2", "value2" }
            },
            sensitive: new HashSet<string> { "sensitive1", "sensitive2" }
        );

        var metadata2 = CreateMetadata(
            tags: new Dictionary<string, ISet<string>>
            {
                { "key2", new HashSet<string> { "value4", "value3" } },
                { "key1", new HashSet<string> { "value2", "value1" } },
            },
            properties: new Dictionary<string, string>
            {
                { "key2", "value2" },
                { "key1", "value1" },
            },
            sensitive: new HashSet<string> { "sensitive2", "sensitive1" }
        );

        Assert.True(metadata1.Equals(metadata2));
        Assert.True(metadata2.Equals(metadata1));
        Assert.Equal(metadata1.GetHashCode(), metadata2.GetHashCode());
    }

    [Fact]
    public void MetadataEqualsShouldBeFalseWhenTagsDiffer()
    {
        var metadata1 = CreateMetadata(
            tags: new Dictionary<string, ISet<string>> { { "key", new HashSet<string> { "a", "b" } } });

        var metadata2 = CreateMetadata(
            tags: new Dictionary<string, ISet<string>> { { "key", new HashSet<string> { "a", "c" } } });

        Assert.NotEqual(metadata1, metadata2);
        Assert.NotEqual(metadata1.GetHashCode(), metadata2.GetHashCode());
    }

    [Fact]
    public void MetadataEqualsShouldBeFalseWhenPropertiesDiffer()
    {
        var metadata1 = CreateMetadata(
            properties: new Dictionary<string, string> { { "key", "value1" } });

        var metadata2 = CreateMetadata(
            properties: new Dictionary<string, string> { { "key", "value2" } });

        Assert.NotEqual(metadata1, metadata2);
        Assert.NotEqual(metadata1.GetHashCode(), metadata2.GetHashCode());
    }

    [Fact]
    public void MetadataEqualsShouldBeFalseWhenSensitiveSetDiffers()
    {
        var metadata1 = CreateMetadata(
            sensitive: new HashSet<string> { "secret1" });

        var metadata2 = CreateMetadata(
            sensitive: new HashSet<string> { "secret2" });

        Assert.NotEqual(metadata1, metadata2);
        Assert.NotEqual(metadata1.GetHashCode(), metadata2.GetHashCode());
    }

    [Fact]
    public void MetadataEqualsShouldBeFalseWhenComparedToNull()
    {
        var metadata = CreateMetadata();
        Assert.False(metadata.Equals(null));
    }

    [Fact]
    public void MetadataEqualsShouldBeFalseWhenComparedToDifferentType()
    {
        var metadata = CreateMetadata();

        // ReSharper disable once SuspiciousTypeConversion.Global
        Assert.False(metadata.Equals("not metadata"));
    }

    [Fact]
    public void MetadataEqualsShouldBeTrueWhenComparedToSelf()
    {
        var metadata = CreateMetadata();
        Assert.True(metadata.Equals(metadata));
    }

    private static Metadata CreateMetadata(
        Dictionary<string, ISet<string>> tags = null,
        Dictionary<string, string> properties = null,
        HashSet<string> sensitive = null)
    {
        return new Metadata(
            tags ?? new Dictionary<string, ISet<string>>(),
            properties ?? new Dictionary<string, string>(),
            sensitive ?? new HashSet<string>()
        );
    }
}