using System.Text;


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

            for (int i=0; i<Replicas.Length; ++i)
            {
                result.Append($" {Replicas[i]}");
                if (i != Replicas.Length - 1)
                {
                    result.Append(",");
                }
            }

            result.Append(" ], \"InSyncReplicas\": [");

            for (int i=0; i<InSyncReplicas.Length; ++i)
            {
                result.Append($" {InSyncReplicas[i]}");
                if (i != InSyncReplicas.Length - 1)
                {
                    result.Append(",");
                }
            }

            result.Append($" ], \"Error\": \"{Error.ToString()}\" }}");

            return result.ToString();
        }
    }

}