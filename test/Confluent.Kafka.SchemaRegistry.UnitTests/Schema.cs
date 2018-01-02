using System;
using Xunit;
using Confluent.Kafka.SchemaRegistry;

namespace Confluent.Kafka.SchemaRegistry.UnitTests
{
    public class SchemaTests
    {
        [Fact]
        public void Construct()
        {
            var schema = new Schema("test-key", 1, 42, "test-schema-string");
            Assert.Equal("test-key", schema.Subject);
            Assert.Equal(1, schema.Version);
            Assert.Equal(42, schema.Id);
            Assert.Equal("test-schema-string", schema.SchemaString);
        }

        [Fact]
        public void ToStringTest()
        {
            var schema = new Schema("test-key", 1, 42, "test-schema-string");    
            Assert.True(schema.ToString().Contains("test-key"));
            Assert.True(schema.ToString().Contains("1"));
            Assert.True(schema.ToString().Contains("42"));
            Assert.True(schema.ToString().Contains("test-schema-string"));
        }

        [Fact]
        public void GetHashCodeTest()
        {
            var schema1 = new Schema("test-key", 1, 42, "test-schema-string");
            var schema2 = new Schema("test-value", 1, 42, "test-schema-string");
            Assert.NotEqual(schema1.GetHashCode(), schema2.GetHashCode());
        }

        [Fact]
        public void CompareTo_Same()
        {
            var schema1 = new Schema("test-key", 1, 42, "test-schema-string");
            var schema2 = new Schema("test-key", 1, 42, "test-schema-string");

            Assert.Equal(0, schema1.CompareTo(schema2));
        }

        [Fact]
        public void CompareToTests()
        {
            // Note: the id and schema strings are not valid, however checking this
            // is considered outside the scope of the CompareTo function.

            var schema1 = new Schema("test-a", 1, 42, "test-schema-string");
            var schema2 = new Schema("test-b", 2, 42, "test-schema-string");
            Assert.True(schema1.CompareTo(schema2) < 0);
            Assert.True(schema2.CompareTo(schema1) > 0);

            var schema3 = new Schema("test-c", 4, 42, "test-schema-string");
            var schema4 = new Schema("test-d", 3, 42, "test-schema-string");
            Assert.True(schema3.CompareTo(schema4) < 0);
            Assert.True(schema4.CompareTo(schema3) > 0);

            var schema5 = new Schema("test-b", 2, 42, "test-schema-string");
            Assert.Equal(0, schema2.CompareTo(schema5));

            var schema6 = new Schema("test-a", 2, 42, "test-schema-string");
            Assert.True(schema1.CompareTo(schema6) < 0);
            Assert.True(schema6.CompareTo(schema1) > 0);

            var schema7 = new Schema("test-b", 1, 42, "test-schema-string");
            Assert.True(schema1.CompareTo(schema7) < 0);
            Assert.True(schema7.CompareTo(schema1) > 0);
        }

        [Fact]
        public void EqualsTests()
        {
            var schema1 = new Schema("test-a", 1, 42, "test-schema-string");
            var schema2 = new Schema("test-b", 1, 42, "test-schema-string");
            var schema3 = new Schema("test-a", 2, 42, "test-schema-string");
            var schema4 = new Schema("test-a", 1, 44, "test-schema-string");
            var schema5 = new Schema("test-a", 1, 42, "test-schema-string2");
            var schema6 = new Schema("test-a", 1, 42, "test-schema-string");

            Assert.NotEqual(schema1, schema2);
            Assert.NotEqual(schema1, schema3);
            Assert.NotEqual(schema1, schema4);
            Assert.NotEqual(schema1, schema5);
            Assert.Equal(schema1, schema6);

            Assert.False(schema1.Equals(schema2));
            Assert.False(schema1.Equals(schema3));
            Assert.False(schema1.Equals(schema4));
            Assert.False(schema1.Equals(schema5));
            Assert.True(schema1.Equals(schema6));
        }
    }
}
