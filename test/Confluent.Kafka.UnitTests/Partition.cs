// Copyright 2018 Confluent Inc.
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
    public class PartitionTests
    {
        [Fact]
        public void SpecialValues()
        {
            Assert.Equal(Partition.Any.Value, -1);
        }

        [Fact]
        public void Constructor()
        {
            Assert.Equal(42, new Partition(42).Value);
        }

        [Fact]
        public void Casts()
        {
            int partitionValue = new Partition(42);
            Assert.Equal(42, partitionValue);

            Partition partition = 42;
            Assert.Equal(partition, new Partition(42));
        }

        [Fact]
        public void Equality()
        {
            Assert.Equal(new Partition(42), new Partition(42));
            Assert.NotEqual(new Partition(42), new Partition(37));
            Assert.True(new Partition(42) == new Partition(42));
            Assert.True(new Partition(42) != new Partition(37));
        }

        [Fact]
        public void Inequality()
        {
            Partition a = new Partition(42);
            Partition a2 = new Partition(42);
            Partition b = new Partition(37);
            Assert.True(b < a);
            Assert.True(a > b);
            Assert.True(a >= b);
            Assert.True(b <= a);
            Assert.True(a <= a2);
            Assert.True(a >= a2);
        }

        [Fact]
        public void Hash()
        {
            Partition partition = new Partition(42);
            Assert.Equal(partition.GetHashCode(), 42.GetHashCode());
        }

        [Fact]
        public void IsSpecial()
        {
            Assert.False(new Partition(42).IsSpecial);
            Assert.False(new Partition(-42).IsSpecial);
            Assert.True(Partition.Any.IsSpecial);
        }

        [Fact]
        public void ToStringTest()
        {
            Assert.Contains(42.ToString(), new Partition(42).ToString());
            Assert.Contains((-42).ToString(), new Partition(-42).ToString());
            Assert.Contains("Any", Partition.Any.ToString());
        }
    }
}
