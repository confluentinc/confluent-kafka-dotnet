namespace Confluent.Kafka
{
    public sealed class Null
    {
        public static Null Instance = new Null();

        private Null() {}
    }
}
