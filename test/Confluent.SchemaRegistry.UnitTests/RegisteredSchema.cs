// Copyright 20 Confluent Inc.
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
using System.Collections.Generic;


namespace Confluent.SchemaRegistry.UnitTests
{
    public class SchemaTests
    {
        [Fact]
        public void Construct()
        {
            var schema = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.Equal("test-key", schema.Subject);
            Assert.Equal(1, schema.Version);
            Assert.Equal(42, schema.Id);
            Assert.Equal("test-schema-string", schema.SchemaString);
            Assert.Empty(schema.References);
            Assert.Equal(SchemaType.Avro, schema.SchemaType);
        }

        [Fact]
        public void Construct2()
        {
            var schema = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Protobuf, new List<SchemaReference> { new SchemaReference("schema-name", "subj", 4) });
            Assert.Equal("test-key", schema.Subject);
            Assert.Equal(1, schema.Version);
            Assert.Equal(42, schema.Id);
            Assert.Equal("test-schema-string", schema.SchemaString);
            Assert.Equal(SchemaType.Protobuf, schema.SchemaType);
            Assert.Single(schema.References);
        }

        [Fact]
        public void ToStringTest()
        {
            var schema = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.Contains("test-key", schema.ToString());
            Assert.Contains("1", schema.ToString());
            Assert.Contains("42", schema.ToString());
        }

        [Fact]
        public void GetHashCodeTest()
        {
            var schema1 = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Json, new List<SchemaReference> { new SchemaReference("a", "b", 3) });
            var schema2 = new RegisteredSchema("test-value", 1, 42, "test-schema-string", SchemaType.Json, new List<SchemaReference> { new SchemaReference("a", "b", 3) });
            // (very unlikely)
            Assert.NotEqual(schema1.GetHashCode(), schema2.GetHashCode());
        }

        [Fact]
        public void CompareTo_Same()
        {
            var schema1 = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Protobuf, new List<SchemaReference>());
            var schema2 = new RegisteredSchema("test-key", 1, 42, "test-schema-string", SchemaType.Protobuf, new List<SchemaReference>());

            Assert.Equal(0, schema1.CompareTo(schema2));
        }

        [Fact]
        public void CompareToTests()
        {
            // Note: the id and schema strings are not valid, however checking this
            // is considered outside the scope of the CompareTo function.

            var schema1 = new RegisteredSchema("test-a", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema2 = new RegisteredSchema("test-b", 2, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.True(schema1.CompareTo(schema2) < 0);
            Assert.True(schema2.CompareTo(schema1) > 0);

            var schema3 = new RegisteredSchema("test-c", 4, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema4 = new RegisteredSchema("test-d", 3, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.True(schema3.CompareTo(schema4) < 0);
            Assert.True(schema4.CompareTo(schema3) > 0);

            var schema5 = new RegisteredSchema("test-b", 2, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.Equal(0, schema2.CompareTo(schema5));

            var schema6 = new RegisteredSchema("test-a", 2, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.True(schema1.CompareTo(schema6) < 0);
            Assert.True(schema6.CompareTo(schema1) > 0);

            var schema7 = new RegisteredSchema("test-b", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            Assert.True(schema1.CompareTo(schema7) < 0);
            Assert.True(schema7.CompareTo(schema1) > 0);
        }

        [Fact]
        public void EqualsTests()
        {
            var schema1 = new RegisteredSchema("test-a", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema2 = new RegisteredSchema("test-b", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema3 = new RegisteredSchema("test-a", 2, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema4 = new RegisteredSchema("test-a", 1, 44, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());
            var schema5 = new RegisteredSchema("test-a", 1, 42, "test-schema-string2", SchemaType.Avro, new List<SchemaReference>());
            var schema6 = new RegisteredSchema("test-a", 1, 42, "test-schema-string", SchemaType.Avro, new List<SchemaReference>());

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
