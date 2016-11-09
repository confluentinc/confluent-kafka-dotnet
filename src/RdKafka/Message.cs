namespace RdKafka
{
    public struct Message
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public byte[] Payload { get; set; }
        public byte[] Key { get; set; }

        public TopicPartitionOffset TopicPartitionOffset =>
            new TopicPartitionOffset()
            {
                Topic = Topic,
                Partition = Partition,
                Offset = Offset
            };
    }

    public struct MessageAndError
    {
        public Message Message;
        public ErrorCode Error;
    }
}
