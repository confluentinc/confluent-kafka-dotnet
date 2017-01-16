using Xunit;


namespace Confluent.Kafka.Tests
{
    public class PartitionMetadataTests
    {
        [Fact]
        public void Constuctor()
        {
            int[] replicas = new int[0];
            int[] inSyncReplicas = new int[0];
            var pm = new PartitionMetadata(33, 1, replicas, inSyncReplicas, ErrorCode._ALL_BROKERS_DOWN);
            Assert.Equal(pm.PartitionId, 33);
            Assert.Equal(pm.Leader, 1);
            Assert.Same(pm.Replicas, replicas);
            Assert.Same(pm.InSyncReplicas, inSyncReplicas);
            Assert.Equal(pm.Error, new Error(ErrorCode._ALL_BROKERS_DOWN));
        }

        // TODO: ToString() tests. Note: there is coverage of this already in the Metdata integration test.
    }
}
