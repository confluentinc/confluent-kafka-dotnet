using System.Collections.Generic;


namespace Confluent.Kafka
{
    public struct CommittedOffsets
    {
        public CommittedOffsets(IList<TopicPartitionOffset> offsets, ErrorCode error)
        {
            Offsets = offsets;
            Error = error;
        }

        // TODO: This should probably be Error (and include a message version of ErrorCode).
        public ErrorCode Error { get; }
        public IList<TopicPartitionOffset> Offsets { get; }
    }
}
