using System;
using Xunit;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Tests
{
    public class IntSerdeTests
    {
        private static readonly int[] toTest = new int[]
        {
            0, 1, -1, 42, -42, 127, 128, 129, -127, -128, -129,
            254, 255, 256, 257, -254, -255, -256, -257,
            (int)short.MinValue-1, (int)short.MinValue, (int)short.MinValue+1,
            (int)short.MaxValue-1, (int)short.MaxValue, (int)short.MaxValue+1,
            int.MaxValue-1, int.MaxValue, int.MinValue, int.MinValue + 1
        };

        [Fact]
        public void IsBigEndian()
        {
            var serializer = new IntSerializer();
            var bytes = serializer.Serialize(42);
            Assert.Equal(bytes.Length, 4);
            // most significant byte in smallest address.
            Assert.Equal(bytes[0], 0);
            Assert.Equal(bytes[3], 42);
        }

        [Fact]
        public void SerializationAgreesWithSystemNetHostToNetworkOrder()
        {
            foreach (int theInt in toTest)
            {
                int networkOrder = System.Net.IPAddress.HostToNetworkOrder(theInt);
                var bytes1 = BitConverter.GetBytes(networkOrder);

                var serializer = new IntSerializer();
                var bytes2 = serializer.Serialize(theInt);

                Assert.Equal(bytes1.Length, bytes2.Length);

                for (int i=0; i<bytes1.Length; ++i)
                {
                    Assert.Equal(bytes1[i], bytes2[i]);
                }
            }
        }

        [Fact]
        public void CanReconstruct()
        {
            var serializer = new IntSerializer();
            var deserializer = new IntDeserializer();

            foreach (int theInt in toTest)
            {
                var reconstructed = deserializer.Deserialize(serializer.Serialize(theInt));
                Assert.Equal(theInt, reconstructed);
            }
        }
    }

}
