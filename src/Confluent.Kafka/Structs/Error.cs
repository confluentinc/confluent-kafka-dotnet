namespace Confluent.Kafka
{
    public struct Error
    {
        public Error(ErrorCode code)
        {
            Code = code;
        }

        public ErrorCode Code { get; }

        // Note: In most practical scenarios there is no benefit to caching this +
        //       significant cost in keeping the extra string reference around.
        public string Message
            => Internal.Util.Marshal.PtrToStringUTF8(Impl.LibRdKafka.err2str(Code));

        public bool HasError
            => Code != ErrorCode.NO_ERROR;

        // TODO: questionably too tricky?
        public static implicit operator bool(Error e)
            => e.HasError;

        public static implicit operator ErrorCode(Error e)
            => e.Code;

        public static implicit operator Error(ErrorCode c)
            => new Error(c);

        public override bool Equals(object obj)
        {
            if (!(obj is Error))
            {
                return false;
            }

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
