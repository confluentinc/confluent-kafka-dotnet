using Xunit;

namespace Confluent.Kafka.UnitTests
{
    public class ConfigEnumTests
    {
        [Fact]
        public void ConsumerEnumProperties()
        {
            var config = new ConsumerConfig
            {
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var value = config.AutoOffsetReset;

            Assert.Equal(AutoOffsetReset.Earliest, value);
        }

        [Fact]
        public void ProducerEnumProperties()
        {
            var config = new ProducerConfig
            {
                Partitioner = Partitioner.Consistent
            };

            Assert.Equal(Partitioner.Consistent, config.Partitioner);
        }

        [Fact]
        public void AcksProperty()
        {
            // standard values
            var config1 = new ProducerConfig { Acks = Acks.None };
            var config2 = new ProducerConfig { Acks = Acks.Leader };
            var config3 = new ProducerConfig { Acks = Acks.All };

            // any numerical value is also ok but needs to be cast.
            // note: values are not range checked by the config class
            // (but are by librdkafka)
            var config4 = new ProducerConfig { Acks = (Acks)1 };
            var config5 = new ProducerConfig { Acks = (Acks)2 };  // this is fine so future proof - enums have exactly the semantics we want.
            var config6 = new ProducerConfig { Acks = (Acks)(-1) };

            Assert.Equal("0", config1.Get("acks"));
            Assert.Equal("1", config2.Get("acks"));
            Assert.Equal("-1", config3.Get("acks"));
            Assert.Equal("1", config4.Get("acks"));
            Assert.Equal("2", config5.Get("acks"));
            Assert.Equal("-1", config6.Get("acks"));

            Assert.Equal(Acks.None, config1.Acks);
            Assert.Equal(Acks.Leader, config2.Acks);
            Assert.Equal(Acks.All, config3.Acks);
            Assert.Equal(Acks.Leader, config4.Acks);
            Assert.Equal((Acks)2, config5.Acks);
            Assert.Equal(Acks.All, config6.Acks);
        }

        [Fact]
        public void Substituted()
        {
            var config1 = new ProducerConfig { SecurityProtocol = SecurityProtocol.SaslPlaintext };
            var config2 = new ProducerConfig { SecurityProtocol = SecurityProtocol.SaslSsl };
            var config3 = new ProducerConfig { Partitioner = Partitioner.ConsistentRandom };
            var config4 = new ProducerConfig { Partitioner = Partitioner.Murmur2Random };
            var config5 = new ConsumerConfig { PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin };

            Assert.Equal("sasl_plaintext", config1.Get("security.protocol"));
            Assert.Equal("sasl_ssl", config2.Get("security.protocol"));
            Assert.Equal("consistent_random", config3.Get("partitioner"));
            Assert.Equal("murmur2_random", config4.Get("partitioner"));
            Assert.Equal("roundrobin", config5.Get("partition.assignment.strategy"));

            Assert.Equal(SecurityProtocol.SaslPlaintext, config1.SecurityProtocol);
            Assert.Equal(SecurityProtocol.SaslSsl, config2.SecurityProtocol);
            Assert.Equal(Partitioner.ConsistentRandom, config3.Partitioner);
            Assert.Equal(Partitioner.Murmur2Random, config4.Partitioner);
            Assert.Equal(PartitionAssignmentStrategy.RoundRobin, config5.PartitionAssignmentStrategy);
        }

        [Fact]
        public void CompileTimeCheck()
        {
            // Set a value for every enum. This tests that ConfigGen 
            // isn't missing any (compile time check).
            var pConfig = new ProducerConfig
            {
                CompressionType = CompressionType.Lz4,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                Partitioner = Partitioner.Murmur2,
                BrokerAddressFamily = BrokerAddressFamily.V4,
                SaslMechanism = SaslMechanism.ScramSha256,
                Acks = Acks.Leader,
            };

            var cConfig = new ConsumerConfig
            {
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                AutoOffsetReset = AutoOffsetReset.Latest,
                BrokerAddressFamily = BrokerAddressFamily.V6,
                SaslMechanism = SaslMechanism.Plain
            };
        }
    }
}
