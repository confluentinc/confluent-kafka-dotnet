using System;
using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder for <see cref="RawProducer"/>. Inherits all configuration and
    ///     handler setters from <see cref="ProducerBuilder{TKey, TValue}"/> bound to
    ///     <see cref="Ignore"/>, <see cref="Ignore"/>.
    /// </summary>
    public class RawProducerBuilder : ProducerBuilder<Ignore, Ignore>
    {
        internal RawDeliveryReportHandler RawDeliveryReportHandler { get; private set; }
        internal RawStatisticsHandler RawStatisticsHandler { get; private set; }

        /// <summary>
        ///     Initialize a new <see cref="RawProducerBuilder"/> with the given config.
        /// </summary>
        public RawProducerBuilder(IEnumerable<KeyValuePair<string, string>> config) : base(config)
        {
            // The base producer demands serializers for TKey/TValue at construction,
            // even though RawProduce bypasses serialization entirely. Wire up a no-op
            // so construction succeeds.
            base.SetKeySerializer(IgnoreSerializer.Instance);
            base.SetValueSerializer(IgnoreSerializer.Instance);
        }

        private sealed class IgnoreSerializer : ISerializer<Ignore>
        {
            public static readonly IgnoreSerializer Instance = new IgnoreSerializer();
            public byte[] Serialize(Ignore data, SerializationContext context) => null;
        }

        /// <summary>
        ///     Set the handler invoked on every produced message after acknowledgement
        ///     or failure. The signature is currently empty; future revisions may expose
        ///     an allocation-free delivery report payload.
        /// </summary>
        public RawProducerBuilder SetDeliveryReportHandler(RawDeliveryReportHandler handler)
        {
            if (this.RawDeliveryReportHandler != null)
            {
                throw new InvalidOperationException("Delivery report handler may not be specified more than once.");
            }
            this.RawDeliveryReportHandler = handler;
            return this;
        }
        
        
        /// <summary>
        ///     Set the handler to call on librdkafka statistics events, receiving the
        ///     JSON payload as a <see cref="ReadOnlySpan{T}"/> of UTF-8 bytes to avoid
        ///     string allocation on every stats tick.
        /// </summary>
        /// <remarks>
        ///     Enable statistics via the <c>statistics.interval.ms</c> config (disabled
        ///     by default). Mutually exclusive with the inherited
        ///     <see cref="ConsumerBuilder{TKey, TValue}.SetStatisticsHandler"/>.
        /// </remarks>
        public RawProducerBuilder SetStatisticsHandler(RawStatisticsHandler statisticsHandler)
        {
            if (this.RawStatisticsHandler != null || this.StatisticsHandler != null)
            {
                throw new InvalidOperationException("Statistics handler may not be specified more than once.");
            }
            this.RawStatisticsHandler = statisticsHandler;
            // Set a no-op string handler so Consumer.cs registers the native stats
            // callback. Our RawConsumer overrides StatisticsCallback and never invokes
            // this dummy.
            base.SetStatisticsHandler((_, _) => { });
            return this;
        }

        /// <summary>
        ///     Build a new <see cref="IProducer{TKey, TValue}"/>. The returned instance
        ///     is also an <see cref="IRawProducer"/>; prefer <see cref="BuildRaw"/> for
        ///     the typed reference.
        /// </summary>
        public override IProducer<Ignore, Ignore> Build()
        {
            return new RawProducer(this);
        }

        /// <summary>
        ///     Build a new <see cref="IRawProducer"/>.
        /// </summary>
        public IRawProducer BuildRaw()
        {
            return (IRawProducer)Build();
        }
    }
}
