#if NETCOREAPP2_1_OR_GREATER
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    /// <summary>
    /// Extends binary producer with frequently used key types
    /// </summary>
    public static class ProducerExtensions
    {
        private const int StackThreshold = 256;

        /// <inheritdoc cref="IProducer.ProduceAsync(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            string topic,
            short key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(short)];

            BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

            return producer.ProduceAsync(topic, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            TopicPartition topicPartition,
            short key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(short)];

            BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

            return producer.ProduceAsync(topicPartition, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.Produce(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            string topic,
            short key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(short)];

            BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

            producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler);
        }


        /// <inheritdoc cref="IProducer.Produce(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            TopicPartition topicPartition,
            short key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(short)];

            BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

            producer.Produce(topicPartition, keyBytes, value, headers, timestamp, deliveryHandler);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            string topic,
            int key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(int)];

            BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

            return producer.ProduceAsync(topic, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            TopicPartition topicPartition,
            int key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(int)];

            BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

            return producer.ProduceAsync(topicPartition, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.Produce(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            string topic,
            int key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(int)];

            BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

            producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler);
        }


        /// <inheritdoc cref="IProducer.Produce(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            TopicPartition topicPartition,
            int key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(int)];

            BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

            producer.Produce(topicPartition, keyBytes, value, headers, timestamp, deliveryHandler);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            string topic,
            long key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(long)];

            BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

            return producer.ProduceAsync(topic, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            TopicPartition topicPartition,
            long key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(long)];

            BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

            return producer.ProduceAsync(topicPartition, keyBytes, value, headers, timestamp, cancellationToken);
        }

        /// <inheritdoc cref="IProducer.Produce(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            string topic,
            long key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(long)];

            BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

            producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler);
        }


        /// <inheritdoc cref="IProducer.Produce(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            TopicPartition topicPartition,
            long key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null)
        {
            Span<byte> keyBytes = stackalloc byte[sizeof(long)];

            BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

            producer.Produce(topicPartition, keyBytes, value, headers, timestamp, deliveryHandler);
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            string topic,
            string key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken),
            Encoding keyEncoding = default)
        {
            keyEncoding = keyEncoding ?? Encoding.UTF8;
            var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
            var allocOnStack = maxBytes <= StackThreshold;
            var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

            var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;
            
            try
            {
                var bytesWritten = keyEncoding.GetBytes(key.AsSpan(), keyBytes);
                
                return producer.ProduceAsync(topic, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, cancellationToken);
            }
            finally
            {
                if (keyBytesPooled != null)
                    ArrayPool<byte>.Shared.Return(keyBytesPooled);
            }
        }

        /// <inheritdoc cref="IProducer.ProduceAsync(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, CancellationToken)" />
        public static Task<DeliveryResult> ProduceAsync(
            this IProducer producer,
            TopicPartition topicPartition,
            string key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            CancellationToken cancellationToken = default(CancellationToken),
            Encoding keyEncoding = default)
        {
            keyEncoding = keyEncoding ?? Encoding.UTF8;
            var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
            var allocOnStack = maxBytes <= StackThreshold;
            var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

            var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;

            try
            {
                var bytesWritten = keyEncoding.GetBytes(key.AsSpan(), keyBytes);

                return producer.ProduceAsync(topicPartition, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, cancellationToken);
            }
            finally
            {
                if (keyBytesPooled != null)
                    ArrayPool<byte>.Shared.Return(keyBytesPooled);
            }
        }

        /// <inheritdoc cref="IProducer.Produce(string, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            string topic,
            string key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null,
            Encoding keyEncoding = default)
        {
            keyEncoding = keyEncoding ?? Encoding.UTF8;
            var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
            var allocOnStack = maxBytes <= StackThreshold;
            var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

            var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;

            try
            {
                var bytesWritten = keyEncoding.GetBytes(key.AsSpan(), keyBytes);

                producer.Produce(topic, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, deliveryHandler);
            }
            finally
            {
                if (keyBytesPooled != null)
                    ArrayPool<byte>.Shared.Return(keyBytesPooled);
            }
        }


        /// <inheritdoc cref="IProducer.Produce(TopicPartition, ReadOnlySpan{byte}, ReadOnlySpan{byte}, Headers, Timestamp, Action{DeliveryReport})" />
        public static void Produce(
            this IProducer producer,
            TopicPartition topicPartition,
            string key,
            ReadOnlySpan<byte> value,
            Headers headers = null,
            Timestamp timestamp = default,
            Action<DeliveryReport> deliveryHandler = null,
            Encoding keyEncoding = default)
        {
            keyEncoding = keyEncoding ?? Encoding.UTF8;
            var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
            var allocOnStack = maxBytes <= StackThreshold;
            var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

            var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;

            try
            {
                var bytesWritten = keyEncoding.GetBytes(key.AsSpan(), keyBytes);

                producer.Produce(topicPartition, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, deliveryHandler);
            }
            finally
            {
                if (keyBytesPooled != null)
                    ArrayPool<byte>.Shared.Return(keyBytesPooled);
            }
        }
    }
}
#endif