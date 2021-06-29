using System.Collections.Generic;

namespace Confluent.Kafka.IntegrationTests
{
    public enum TestProducerType
    {
        KeyValue,
        Binary
    }

    internal sealed class TestProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
    {
        private readonly TestProducerType producerType;

        public TestProducerBuilder(
            IEnumerable<KeyValuePair<string, string>> config,
            TestProducerType producerType) : base(config)
        {
            this.producerType = producerType;
        }

        public override IProducer<TKey, TValue> Build()
        {
            if (producerType == TestProducerType.KeyValue)
                return base.Build();

            var builder = new ProducerBuilder(this.Config);
            builder.SetDefaultPartitioner(this.DefaultPartitioner);

            foreach (var (topic, partitioner) in this.Partitioners)
                builder.SetPartitioner(topic, partitioner);

            builder.SetErrorHandler((p, e) => this.ErrorHandler?.Invoke(new TestProducerAdapter<TKey, TValue>(p), e));
            builder.SetLogHandler((p, m) => this.LogHandler?.Invoke(new TestProducerAdapter<TKey, TValue>(p), m));
            
            builder.SetOAuthBearerTokenRefreshHandler((p, t) =>
                this.OAuthBearerTokenRefreshHandler?.Invoke(new TestProducerAdapter<TKey, TValue>(p), t));
            
            builder.SetStatisticsHandler((p, s) =>
                this.StatisticsHandler?.Invoke(new TestProducerAdapter<TKey, TValue>(p), s));

            return new TestProducerAdapter<TKey, TValue>(builder.Build(), KeySerializer, ValueSerializer,
                AsyncKeySerializer, AsyncValueSerializer);
        }
    }
}