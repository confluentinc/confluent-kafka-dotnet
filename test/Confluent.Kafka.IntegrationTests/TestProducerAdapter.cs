using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.IntegrationTests
{
    internal sealed class TestProducerAdapter<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly IProducer producer;
        private readonly ISerializer<TKey> keySerializer;
        private readonly ISerializer<TValue> valueSerializer;
        private readonly IAsyncSerializer<TKey> asyncKeySerializer;
        private readonly IAsyncSerializer<TValue> asyncValueSerializer;

        public TestProducerAdapter(
            IProducer producer,
            ISerializer<TKey> keySerializer = null,
            ISerializer<TValue> valueSerializer = null,
            IAsyncSerializer<TKey> asyncKeySerializer = null,
            IAsyncSerializer<TValue> asyncValueSerializer = null)
        {
            this.producer = producer;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.asyncKeySerializer = asyncKeySerializer;
            this.asyncValueSerializer = asyncValueSerializer;
        }

        public void Dispose() => producer.Dispose();

        public Handle Handle => producer.Handle;

        public string Name => producer.Name;

        public int AddBrokers(string brokers) => producer.AddBrokers(brokers);

        public void SetSaslCredentials(string username, string password)
            => producer.SetSaslCredentials(username, password);

        public void InitTransactions(TimeSpan timeout) => producer.InitTransactions(timeout);

        public void BeginTransaction() => producer.BeginTransaction();

        public void CommitTransaction(TimeSpan timeout) => producer.CommitTransaction(timeout);

        public void CommitTransaction() => producer.CommitTransaction();

        public void AbortTransaction(TimeSpan timeout) => producer.AbortTransaction(timeout);

        public void AbortTransaction() => producer.AbortTransaction();

        public void SendOffsetsToTransaction(
            IEnumerable<TopicPartitionOffset> offsets,
            IConsumerGroupMetadata groupMetadata,
            TimeSpan timeout)
            => producer.SendOffsetsToTransaction(offsets, groupMetadata, timeout);

        public int Poll(TimeSpan timeout) => producer.Poll(timeout);

        public int Flush(TimeSpan timeout) => producer.Flush(timeout);

        public void Flush(CancellationToken cancellationToken = default) => producer.Flush(cancellationToken);

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default)
        {
            message.Headers = message.Headers ?? new Headers();

            var result = await producer.ProduceAsync(
                topic,
                GetKeyBytes(message.Key, new SerializationContext(MessageComponentType.Key, topic, message.Headers)),
                GetValueBytes(message.Value,
                    new SerializationContext(MessageComponentType.Value, topic, message.Headers)),
                message.Headers,
                message.Timestamp,
                cancellationToken);

            return new DeliveryResult<TKey, TValue>
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Status = result.Status,
                Message = new Message<TKey, TValue>()
                {
                    Headers = result.Headers,
                    Timestamp = result.Timestamp,
                    Key = message.Key,
                    Value = message.Value
                }
            };
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default)
        {
            message.Headers = message.Headers ?? new Headers();
            var result = await producer.ProduceAsync(
                topicPartition,
                GetKeyBytes(message.Key,
                    new SerializationContext(MessageComponentType.Key, topicPartition.Topic, message.Headers)),
                GetValueBytes(message.Value,
                    new SerializationContext(MessageComponentType.Value, topicPartition.Topic, message.Headers)),
                message.Headers,
                message.Timestamp,
                cancellationToken);

            return new DeliveryResult<TKey, TValue>
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Status = result.Status,
                Message = new Message<TKey, TValue>()
                {
                    Headers = result.Headers,
                    Timestamp = result.Timestamp,
                    Key = message.Key,
                    Value = message.Value
                }
            };
        }

        public void Produce(
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            message.Headers = message.Headers ?? new Headers();
            producer.Produce(
                topic,
                GetKeyBytes(message.Key, new SerializationContext(MessageComponentType.Key, topic, message.Headers)),
                GetValueBytes(message.Value,
                    new SerializationContext(MessageComponentType.Value, topic, message.Headers)),
                message.Headers,
                message.Timestamp,
                deliveryHandler != null
                    ? report => deliveryHandler(new DeliveryReport<TKey, TValue>
                    {
                        TopicPartitionOffsetError = report.TopicPartitionOffsetError,
                        Status = report.Status,
                        Message = new Message<TKey, TValue>()
                        {
                            Headers = report.Headers,
                            Timestamp = report.Timestamp,
                            Key = message.Key,
                            Value = message.Value
                        }
                    })
                    : default(Action<DeliveryReport>));
        }

        public void Produce(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            message.Headers = message.Headers ?? new Headers();

            producer.Produce(
                topicPartition,
                GetKeyBytes(message.Key,
                    new SerializationContext(MessageComponentType.Key, topicPartition.Topic, message.Headers)),
                GetValueBytes(message.Value,
                    new SerializationContext(MessageComponentType.Value, topicPartition.Topic, message.Headers)),
                message.Headers,
                message.Timestamp,
                deliveryHandler != null
                    ? report => deliveryHandler(new DeliveryReport<TKey, TValue>
                    {
                        TopicPartitionOffsetError = report.TopicPartitionOffsetError,
                        Status = report.Status,
                        Message = new Message<TKey, TValue>()
                        {
                            Headers = report.Headers,
                            Timestamp = report.Timestamp,
                            Key = message.Key,
                            Value = message.Value
                        }
                    })
                    : default(Action<DeliveryReport>));
        }


        private ReadOnlySpan<byte> GetKeyBytes(TKey key, SerializationContext context)
        {
            if (keySerializer != null)
                return keySerializer.Serialize(key, context);

            if (asyncKeySerializer != null)
                return asyncKeySerializer.SerializeAsync(key, context).GetAwaiter().GetResult();

            return ToBytes(key);
        }

        private ReadOnlySpan<byte> GetValueBytes(TValue value, SerializationContext context)
        {
            if (valueSerializer != null)
                return valueSerializer.Serialize(value, context);

            if (asyncValueSerializer != null)
                return asyncValueSerializer.SerializeAsync(value, context).GetAwaiter().GetResult();

            return ToBytes(value);
        }


        private ReadOnlySpan<byte> ToBytes<T>(T value)
        {
            if (value == null || value is Null)
                return ReadOnlySpan<byte>.Empty;

            switch (value)
            {
                case byte[] v: return Serializers.ByteArray.Serialize(v, SerializationContext.Empty);
                case string v: return Serializers.Utf8.Serialize(v, SerializationContext.Empty);
                case int v: return Serializers.Int32.Serialize(v, SerializationContext.Empty);
                case long v: return Serializers.Int64.Serialize(v, SerializationContext.Empty);
                case double v: return Serializers.Double.Serialize(v, SerializationContext.Empty);
            }

            throw new NotSupportedException(typeof(T).ToString());
        }
    }
}