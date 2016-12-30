using Xunit;


namespace Confluent.Kafka.Tests
{
    public class OffsetTests
    {
        [Fact]
        public void SpecialValues()
        {
            Assert.Equal(Offset.Beginning.Value, -2);
            Assert.Equal(Offset.End.Value, -1);
            Assert.Equal(Offset.Invalid.Value, -1001);
            Assert.Equal(Offset.Stored.Value, -1000);
        }

        [Fact]
        public void Constructor()
        {
            Assert.Equal(new Offset(42).Value, 42);
        }

        [Fact]
        public void Casts()
        {
            long offsetValue = new Offset(42);
            Assert.Equal(offsetValue, 42);

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
            Assert.True(Offset.Invalid.IsSpecial);
            Assert.True(Offset.Stored.IsSpecial);
        }

        [Fact]
        public void ToStringTest()
        {
            Assert.Equal(new Offset(42).ToString(), 42.ToString());
            Assert.Equal(new Offset(-42).ToString(), (-42).ToString());
            Assert.True(Offset.Invalid.ToString().Contains("Invalid"));
            Assert.True(Offset.Invalid.ToString().Contains((-1001).ToString()));
            Assert.True(Offset.Stored.ToString().Contains("Stored"));
            Assert.True(Offset.Stored.ToString().Contains((-1000).ToString()));
            Assert.True(Offset.Beginning.ToString().Contains("Beginning"));
            Assert.True(Offset.Beginning.ToString().Contains((-2).ToString()));
            Assert.True(Offset.End.ToString().Contains("End"));
            Assert.True(Offset.End.ToString().Contains((-1).ToString()));
        }
    }
}
