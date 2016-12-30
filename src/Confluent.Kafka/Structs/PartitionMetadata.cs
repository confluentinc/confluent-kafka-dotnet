using System.Text;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka topic partition.
    /// </summary>
    public struct PartitionMetadata
    {
        public int PartitionId { get; set; }
        public int Leader { get; set; }
        public int[] Replicas { get; set; }
        public int[] InSyncReplicas { get; set; }
        public ErrorCode Error { get; set; }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{ \"PartitionId\": {PartitionId}, \"Leader\": {Leader}, \"Replicas\": [");
            result.Append(string.Join(",", Replicas.Select(r => $" {r.ToString()}")));
            result.Append(" ], \"InSyncReplicas\": [");
            result.Append(string.Join(",", InSyncReplicas.Select(r => $" {r.ToString()}")));
            result.Append($" ], \"Error\": \"{Error.ToString()}\" }}");
            return result.ToString();
        }
    }

}