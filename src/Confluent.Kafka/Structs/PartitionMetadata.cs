using System.Text;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka topic partition.
    /// </summary>
    public struct PartitionMetadata
    {
        public PartitionMetadata(int partitionId, int leader, int[] replicas, int[] inSyncReplicas, Error error)
        {
            PartitionId = partitionId;
            Leader = leader;
            Replicas = replicas;
            InSyncReplicas = inSyncReplicas;
            Error = error;
        }

        public int PartitionId { get; }
        public int Leader { get; }
        public int[] Replicas { get; }
        public int[] InSyncReplicas { get; }
        public Error Error { get; }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"PartitionId\": {PartitionId}, \"Leader\": {Leader}, \"Replicas\": [");
            result.Append(string.Join(",", Replicas.Select(r => $" {r.ToString()}")));
            result.Append(" ], \"InSyncReplicas\": [");
            result.Append(string.Join(",", InSyncReplicas.Select(r => $" {r.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.Code.ToString()}\" }}");
            return result.ToString();
        }
    }

}