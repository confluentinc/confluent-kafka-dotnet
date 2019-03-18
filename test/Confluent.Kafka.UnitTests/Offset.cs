// Copyright 2016-2017 Confluent Inc.
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


namespace Confluent.Kafka.UnitTests
{
    public class OffsetTests
    {
        [Fact]
        public void SpecialValues()
        {
            Assert.Equal(Offset.Beginning.Value, -2);
            Assert.Equal(Offset.End.Value, -1);
            Assert.Equal(Offset.Unset.Value, -1001);
            Assert.Equal(Offset.Stored.Value, -1000);
        }

        [Fact]
        public void Constructor()
        {
            Assert.Equal(42, new Offset(42).Value);
        }

        [Fact]
        public void Casts()
        {
            long offsetValue = new Offset(42);
            Assert.Equal(42, offsetValue);

            Offset offset = 42;
            Assert.Equal(offset, new Offset(42));
        }

        [Fact]
        public void Equality()
        {
            Assert.Equal(new Offset(42), new Offset(42));
            Assert.NotEqual(new Offset(42), new Offset(37));
            Assert.True(new Offset(42) == new Offset(42));
            Assert.True(new Offset(42) != new Offset(37));
        }

        [Fact]
        public void Inequality()
        {
            Offset a = new Offset(42);
            Offset a2 = new Offset(42);
            Offset b = new Offset(37);
            Assert.True(b < a);
            Assert.True(a > b);
            Assert.True(a >= b);
            Assert.True(b <= a);
            Assert.True(a <= a2);
            Assert.True(a >= a2);
        }

        [Fact]
        public void Addition_Int()
        {
            Offset a = new Offset(42);
            Assert.Equal(45, a + 3);
        }

        [Fact]
        public void Addition_Long()
        {
            Offset a = new Offset(42);
            Assert.Equal(100, a + (long)58);
        }

        [Fact]
        public void Hash()
        {
            Offset offset = new Offset(42);
            Assert.Equal(offset.GetHashCode(), 42.GetHashCode());
        }

        [Fact]
        public void IsSpecial()
        {
            Assert.False(new Offset(42).IsSpecial);
            Assert.False(new Offset(-42).IsSpecial);
            Assert.True(Offset.Beginning.IsSpecial);
            Assert.True(Offset.End.IsSpecial);
            Assert.True(Offset.Unset.IsSpecial);
            Assert.True(Offset.Stored.IsSpecial);
        }

        [Fact]
        public void ToStringTest()
        {
            Assert.Equal(new Offset(42).ToString(), 42.ToString());
            Assert.Equal(new Offset(-42).ToString(), (-42).ToString());
            Assert.Contains("Unset", Offset.Unset.ToString());
            Assert.Contains((-1001).ToString(), Offset.Unset.ToString());
            Assert.Contains("Stored", Offset.Stored.ToString());
            Assert.Contains((-1000).ToString(), Offset.Stored.ToString());
            Assert.Contains("Beginning", Offset.Beginning.ToString());
            Assert.Contains((-2).ToString(), Offset.Beginning.ToString());
            Assert.Contains("End", Offset.End.ToString());
            Assert.Contains((-1).ToString(), Offset.End.ToString());
        }
    }
}
