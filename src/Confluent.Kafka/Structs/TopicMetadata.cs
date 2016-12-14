using System.Collections.Generic;
using System.Text;


namespace Confluent.Kafka
{

    /// <summary>
    ///     Metadata pertaining to a single Kafka topic.
    /// </summary>
    public struct TopicMetadata
    {
        public string Topic { get; set; }
        public List<PartitionMetadata> Partitions { get; set; }
        public ErrorCode Error { get; set; }

        public override string ToString()
        {
            var result = new StringBuilder();

            result.Append($"{{ \"Topic\": \"{Topic}\", \"Partitions\": [");

            for (int i=0; i<Partitions.Count; ++i)
            {
                result.Append($" {Partitions[i].ToString()}");
                if (i != Partitions.Count-1)
                {
                    result.Append(",");
                }
            }

            result.Append($" ], \"Error\": \"{Error.ToString()}\" }}");

            return result.ToString();
        }
    }
}
