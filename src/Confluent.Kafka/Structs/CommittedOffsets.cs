using System.Collections.Generic;


namespace Confluent.Kafka
{
    public struct CommittedOffsets
    {
        public CommittedOffsets(IList<TopicPartitionOffset> offsets, Error error)
        {
            Offsets = offsets;
            Error = error;
        }

        public Error Error { get; }
        public IList<TopicPartitionOffset> Offsets { get; }
    }
}
