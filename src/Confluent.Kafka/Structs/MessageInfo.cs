namespace Confluent.Kafka
{
    public struct MessageInfo<TKey, TValue>
    {
        public MessageInfo(string topic, int partition, long offset, TKey key, TValue val, Timestamp timestamp, Error error)
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
        {
            get { return new TopicPartitionOffset(Topic, Partition, Offset); }
        }

        public TopicPartition TopicPartition
        {
            get { return new TopicPartition(Topic, Partition); }
        }
    }


    public struct MessageInfo
    {
        public MessageInfo(string topic, int partition, long offset, byte[] key, byte[] val, Timestamp timestamp, Error error)
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
        {
            get { return new TopicPartitionOffset(Topic, Partition, Offset); }
        }

        public TopicPartition TopicPartition
        {
            get { return new TopicPartition(Topic, Partition); }
        }
    }

}
