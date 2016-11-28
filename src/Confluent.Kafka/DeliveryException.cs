using System;


namespace Confluent.Kafka
{
    public class DeliveryException : Exception
    {
        public MessageInfo MessageInfo { get; private set; }

        public DeliveryException(MessageInfo messageInfo)
        {
            MessageInfo = messageInfo;
        }
    }

    public class DeliveryException<TKey, TValue> : Exception
    {
        public MessageInfo<TKey, TValue> MessageInfo { get; private set; }

        public DeliveryException(MessageInfo<TKey, TValue> messageInfo)
        {
            MessageInfo = messageInfo;
        }
    }
}
