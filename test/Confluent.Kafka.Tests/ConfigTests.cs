using System;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka;

namespace Confluent.Kafka.Tests
{
    public class ConfigTests
    {
        [Fact]
        public void SetAndGetParameterWorks()
        {
            var config = new Config();
            config["client.id"] = "test";
            Assert.Equal(config["client.id"], "test");
        }

        [Fact]
        public void SettingUnknownParameterThrows()
        {
            var config = new Config();
            Assert.Throws<InvalidOperationException>(() => config["unknown"] = "something");
        }

        [Fact]
        public void SettingParameterToInvalidValueThrows()
        {
            var config = new Config();
            Assert.Throws<ArgumentException>(() => config["session.timeout.ms"] = "string");
        }

        [Fact]
        public void GettingUnknownParameterThrows()
        {
            var config = new Config();
            Assert.Throws<InvalidOperationException>(() => config["unknown"]);
        }

        [Fact]
        public void DumpedConfigLooksReasonable()
        {
            var config = new Config();
            config["client.id"] = "test";
            Dictionary<string,string> dump = config.Dump();
            Assert.Equal(dump["client.id"], "test");
        }
    }
}
