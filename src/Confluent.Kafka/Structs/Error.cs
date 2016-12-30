namespace Confluent.Kafka
{
    // TODO: ErrorCode (and by extension Error) are closely tied to RdKafka concepts but do not have Rd in the name.
    //       RdKafkaException has the same properties as Error but it does have Rd in the name.
    //       TODO: Work out what is best and be consistent (probably no Rd).
    public struct Error
    {
        public Error(ErrorCode code, string message)
        {
            Message = message;
            Code = code;
        }

        public string Message { get; }
        public ErrorCode Code { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is Error))
            {
                return false;
            }

            // TODO: think carefully about whether not considering .Message here is a flawed idea (i don't think so).
            return ((Error)obj).Code == Code;
        }

        public override int GetHashCode()
            => Code.GetHashCode();

        public static bool operator ==(Error a, Error b)
            => a.Equals(b);

        public static bool operator !=(Error a, Error b)
            => !(a == b);

        public override string ToString()
            => $"[{(int)Code}] {Message}";
    }
}
