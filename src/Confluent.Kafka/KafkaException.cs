using System;


namespace Confluent.Kafka
{
    public class KafkaException : Exception
    {
        public KafkaException(Error error, string message)
            : base(message)
        {
            Error = error;
        }

        public Error Error { get; }
    }
}
