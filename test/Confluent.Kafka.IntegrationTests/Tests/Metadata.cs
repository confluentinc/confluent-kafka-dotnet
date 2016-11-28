using System;
using System.Collections.Generic;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Basic test that metadata request works.
        /// </summary>
        [IntegrationTest]
        public static void Metadata(string bootstrapServers, string topic)
        {
            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            using (var producer = new Producer(producerConfig))
            {
                var metadata = producer.GetMetadata();
                Console.WriteLine(metadata);
                Assert.NotNull(metadata.Brokers);
                Assert.True(metadata.Brokers.Count > 0);
            }
        }
    }
}
