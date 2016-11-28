using System;

namespace Confluent.Kafka
{
    public struct MessageInfo<TKey, TValue>
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public TKey Key { get; set; }
        public TValue Value { get; set; }
        public DateTime Timestamp { get; set; }
        public ErrorCode ErrorCode { get; set; }
        public string ErrorMessage { get; set; }

        public TopicPartitionOffset TopicPartitionOffset
        {
            get
            {
                return new TopicPartitionOffset
                {
                    Topic = Topic,
                    Partition = Partition,
                    Offset = Offset
                };
            }
        }

        public TopicPartition TopicPartition
        {
            get
            {
                return new TopicPartition
                {
                    Topic = Topic,
                    Partition = Partition
                };
            }
        }

    }


    public struct MessageInfo
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public byte[] Value { get; set; }
        public byte[] Key { get; set; }
        public DateTime Timestamp { get; set; }
        public ErrorCode ErrorCode { get; set; }
        public string ErrorMessage { get; set; }

        public TopicPartitionOffset TopicPartitionOffset
        {
            get
            {
                return new TopicPartitionOffset
                {
                    Topic = Topic,
                    Partition = Partition,
                    Offset = Offset
                };
            }
        }

        public TopicPartition TopicPartition
        {
            get
            {
                return new TopicPartition
                {
                    Topic = Topic,
                    Partition = Partition
                };
            }
        }

    }

}