using System.Collections.Generic;


namespace Confluent.Kafka
{
    public struct OffsetCommitArgs
    {
        public ErrorCode Error { get; set; }
        public IList<TopicPartitionOffset> Offsets { get; set; }
    }
}
