using System;

namespace Confluent.Kafka
{
    public struct Message<TKey, TValue>
    {
        public Message(TKey key, TValue value, DateTime? timestamp = null)
        {
            Key = key;
            Value = value;
            Timestamp = timestamp;
        }

        public TKey Key { get; set; }
        public TValue Value { get; set; }
        public DateTime? Timestamp { get; set; }
    }

    public struct Message
    {
        public Message(ArraySegment<byte>? key, ArraySegment<byte>? value, DateTime? timestamp = null)
        {
            Key = key;
            Value = value;
            Timestamp = timestamp;
        }

        public Message(byte[] key, byte[] val, DateTime? timestamp = null)
        {
            Key = key == null ? null : (ArraySegment<byte>?)new ArraySegment<byte>(key);
            Value = val == null ? null : (ArraySegment<byte>?)new ArraySegment<byte>(val);
            Timestamp = timestamp;
        }

        public ArraySegment<byte>? Value { get; set; }
        public ArraySegment<byte>? Key { get; set; }
        public DateTime? Timestamp { get; set; }
    }
}
