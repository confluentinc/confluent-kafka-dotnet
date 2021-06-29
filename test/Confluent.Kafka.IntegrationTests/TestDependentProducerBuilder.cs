namespace Confluent.Kafka.IntegrationTests
{
    internal sealed class TestDependentProducerBuilder<TKey, TValue> : DependentProducerBuilder<TKey, TValue>
    {
        private readonly TestProducerType type;

        public TestDependentProducerBuilder(Handle handle, TestProducerType type) : base(handle)
        {
            this.type = type;
        }

        public override IProducer<TKey, TValue> Build()
        {
            if (type == TestProducerType.KeyValue)
                return base.Build();

            return new TestProducerAdapter<TKey, TValue>(
                new DependentProducerBuilder(Handle).Build(), KeySerializer, ValueSerializer, AsyncKeySerializer, AsyncValueSerializer);
        }
    }
}