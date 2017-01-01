namespace Confluent.Kafka
{
    public struct Message<TKey, TValue>
    {
        public Message(string topic, int partition, long offset, TKey key, TValue val, Timestamp timestamp, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = val;
            Timestamp = timestamp;
            Error = error;
        }

        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        public TKey Key { get; }
        public TValue Value { get; }
        public Timestamp Timestamp { get; }
        public Error Error { get; }

        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
    }


    public struct Message
    {
        public Message(string topic, int partition, long offset, byte[] key, byte[] val, Timestamp timestamp, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = val;
            Timestamp = timestamp;
            Error = error;
        }

        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        public byte[] Value { get; }
        public byte[] Key { get; }
        public Timestamp Timestamp { get; }
        public Error Error { get; }

        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
    }

}
