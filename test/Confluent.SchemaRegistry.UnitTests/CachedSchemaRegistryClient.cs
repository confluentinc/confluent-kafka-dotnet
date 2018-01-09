using System;
using System.Collections.Generic;
using Xunit;
using Confluent.SchemaRegistry;


namespace Confluent.SchemaRegistry.UnitTests
{
    public class CachedSchemaRegistryClientTests
    {
        [Fact]
        public void NullConfig()
        {
            Assert.Throws<ArgumentNullException>(() => new CachedSchemaRegistryClient(null));
        }

        [Fact]
        public void NoUrls()
        {
            var config = new Dictionary<string, object>();
            Assert.Throws<ArgumentException>(() => new CachedSchemaRegistryClient(config));
        }

        [Fact]
        public void ConstructKeySubjectName()
        {
            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" }
            };

            CachedSchemaRegistryClient src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-key", src.ConstructKeySubjectName("mytopic"));
        }

        [Fact]
        public void ConstructValueSubjectName()
        {
            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" }
            };

            CachedSchemaRegistryClient src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-value", src.ConstructValueSubjectName("mytopic"));
        }
    }
}
