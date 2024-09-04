// Copyright 2022 Confluent Inc.
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

using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    public class WildcardMatcherTest
    {
        [Fact]
        public void Match()
        {
            Assert.False(WildcardMatcher.Match(null, "Foo"));
            Assert.False(WildcardMatcher.Match("Foo", null));
            Assert.True(WildcardMatcher.Match(null, null));
            Assert.True(WildcardMatcher.Match("Foo", "Foo"));
            Assert.True(WildcardMatcher.Match("", ""));
            Assert.True(WildcardMatcher.Match("", "*"));
            Assert.False(WildcardMatcher.Match("", "?"));
            Assert.True(WildcardMatcher.Match("Foo", "Fo*"));
            Assert.True(WildcardMatcher.Match("Foo", "Fo?"));
            Assert.True(WildcardMatcher.Match("Foo Bar and Catflap", "Fo*"));
            Assert.True(WildcardMatcher.Match("New Bookmarks", "N?w ?o?k??r?s"));
            Assert.False(WildcardMatcher.Match("Foo", "Bar"));
            Assert.True(WildcardMatcher.Match("Foo Bar Foo", "F*o Bar*"));
            Assert.True(WildcardMatcher.Match("Adobe Acrobat Installer", "Ad*er"));
            Assert.True(WildcardMatcher.Match("Foo", "*Foo"));
            Assert.True(WildcardMatcher.Match("BarFoo", "*Foo"));
            Assert.True(WildcardMatcher.Match("Foo", "Foo*"));
            Assert.True(WildcardMatcher.Match("FooBar", "Foo*"));
            Assert.False(WildcardMatcher.Match("FOO", "*Foo"));
            Assert.False(WildcardMatcher.Match("BARFOO", "*Foo"));
            Assert.False(WildcardMatcher.Match("FOO", "Foo*"));
            Assert.False(WildcardMatcher.Match("FOOBAR", "Foo*"));
            Assert.True(WildcardMatcher.Match("eve", "eve*"));
            Assert.True(WildcardMatcher.Match("alice.bob.eve", "a*.bob.eve"));
            Assert.True(WildcardMatcher.Match("alice.bob.eve", "a*.bob.e*"));
            Assert.False(WildcardMatcher.Match("alice.bob.eve", "a*"));
            Assert.True(WildcardMatcher.Match("alice.bob.eve", "a**"));
            Assert.False(WildcardMatcher.Match("alice.bob.eve", "alice.bob*"));
            Assert.True(WildcardMatcher.Match("alice.bob.eve", "alice.bob**"));
        }
    }
}