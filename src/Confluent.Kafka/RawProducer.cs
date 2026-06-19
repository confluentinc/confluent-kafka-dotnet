using System;
using System.Text;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    ///     A Kafka producer that adds an allocation-free produce path on top of
    ///     <see cref="Producer{TKey, TValue}"/>. Inherits all lifecycle behavior
    ///     (Flush/Dispose/transactions/etc.) from the base producer (bound to
    ///     <see cref="Ignore"/>, <see cref="Ignore"/>).
    /// </summary>
    internal class RawProducer : Producer<Ignore, Ignore>, IRawProducer
    {
        private const int StackNameBufferSize = 256;

        private readonly RawDeliveryReportHandler rawDeliveryReportHandler;
        private readonly RawStatisticsHandler rawStatisticsHandler;

        internal RawProducer(RawProducerBuilder builder) : base(builder)
        {
            this.rawDeliveryReportHandler = builder.RawDeliveryReportHandler;
            this.rawStatisticsHandler = builder.RawStatisticsHandler;
        }
        
        
        protected override unsafe int StatisticsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            if (ownedKafkaHandle.IsClosed) { return 0; }
            try
            {
                rawStatisticsHandler?.Invoke(new ReadOnlySpan<byte>(json.ToPointer(), (int)json_len));
            }
            catch (Exception e)
            {
                handlerException = e;
            }
            return 0;
        }

        /// <summary>
        ///     Explicit interface implementation of the low-level produce entry point.
        ///     Hidden from external callers; used by <see cref="RawProducerMarshal"/>
        ///     and the public overloads below.
        /// </summary>
        ErrorCode IRawProducer.ProduceRawCore(
            string topic,
            int partition,
            IntPtr keyPtr, int keyLen,
            IntPtr valuePtr, int valueLen,
            IntPtr headers,
            IntPtr msgFlags,
            IntPtr opaque)
        {
            return Librdkafka.produceva(
                this.Handle.LibrdkafkaHandle.DangerousGetHandle(),
                topic,
                partition,
                msgFlags,
                valuePtr, (UIntPtr)valueLen,
                keyPtr, (UIntPtr)keyLen,
                0L,
                headers,
                opaque);
        }

        /// <summary>
        ///     Explicit interface implementation — orchestrates header handle
        ///     creation, produce, and cleanup for a produce-with-headers call.
        /// </summary>
        void IRawProducer.ProduceRawWithHeaders(
            string topic,
            int partition,
            IntPtr keyPtr, int keyLen,
            IntPtr valuePtr, int valueLen,
            in KafkaHeaders headers,
            IntPtr msgFlags,
            IntPtr opaque)
        {
            IRawProducer self = this;

            if (headers.Count == 0)
            {
                var bareErr = self.ProduceRawCore(
                    topic, partition,
                    keyPtr, keyLen, valuePtr, valueLen,
                    IntPtr.Zero, msgFlags, opaque);
                ThrowIfError(bareErr);
                return;
            }

            var headersPtr = BuildHeadersHandle(in headers);
            bool ownedByProduce = false;
            try
            {
                var err = self.ProduceRawCore(
                    topic, partition,
                    keyPtr, keyLen, valuePtr, valueLen,
                    headersPtr, msgFlags, opaque);
                ThrowIfError(err);
                ownedByProduce = true;
            }
            finally
            {
                if (!ownedByProduce)
                {
                    Librdkafka.headers_destroy(headersPtr);
                }
            }
        }

        /// <summary>
        ///     Explicit interface implementation — orchestrates header handle
        ///     creation, produce, and cleanup for a produce carrying both header
        ///     collections concatenated into one native list.
        /// </summary>
        void IRawProducer.ProduceRawWithHeaders(
            string topic,
            int partition,
            IntPtr keyPtr, int keyLen,
            IntPtr valuePtr, int valueLen,
            in KafkaHeaders headers1,
            in KafkaHeaders headers2,
            IntPtr msgFlags,
            IntPtr opaque)
        {
            IRawProducer self = this;

            if (headers1.Count + headers2.Count == 0)
            {
                var bareErr = self.ProduceRawCore(
                    topic, partition,
                    keyPtr, keyLen, valuePtr, valueLen,
                    IntPtr.Zero, msgFlags, opaque);
                ThrowIfError(bareErr);
                return;
            }

            var headersPtr = BuildHeadersHandle(in headers1, in headers2);
            bool ownedByProduce = false;
            try
            {
                var err = self.ProduceRawCore(
                    topic, partition,
                    keyPtr, keyLen, valuePtr, valueLen,
                    headersPtr, msgFlags, opaque);
                ThrowIfError(err);
                ownedByProduce = true;
            }
            finally
            {
                if (!ownedByProduce)
                {
                    Librdkafka.headers_destroy(headersPtr);
                }
            }
        }

        /// <summary>
        ///     Builds a native librdkafka headers handle from <paramref name="headers"/>.
        ///     Returns <see cref="IntPtr.Zero"/> if the collection is empty. On any
        ///     failure, destroys the partial handle and throws. On success, ownership
        ///     transfers to the caller — either pass it to produceva (which takes
        ///     ownership on success) or call <c>headers_destroy</c>.
        /// </summary>
        internal static unsafe IntPtr BuildHeadersHandle(in KafkaHeaders headers)
        {
            if (headers.Count == 0) return IntPtr.Zero;

            var ptr = Librdkafka.headers_new((IntPtr)headers.Count);
            if (ptr == IntPtr.Zero)
            {
                throw new KafkaException(new Error(ErrorCode.Local_Fail, "Failed to allocate headers list."));
            }

            try
            {
                Span<byte> nameBuffer = stackalloc byte[StackNameBufferSize];
                AddHeadersToHandle(ptr, in headers, nameBuffer);
            }
            catch
            {
                Librdkafka.headers_destroy(ptr);
                throw;
            }

            return ptr;
        }

        /// <summary>
        ///     Builds a single native librdkafka headers handle holding the
        ///     concatenation of <paramref name="first"/> and <paramref name="second"/>
        ///     (all of <paramref name="first"/>, then all of <paramref name="second"/>).
        ///     Returns <see cref="IntPtr.Zero"/> if both collections are empty. Same
        ///     ownership and failure contract as <see cref="BuildHeadersHandle(in KafkaHeaders)"/>.
        /// </summary>
        internal static unsafe IntPtr BuildHeadersHandle(in KafkaHeaders first, in KafkaHeaders second)
        {
            var total = first.Count + second.Count;
            if (total == 0) return IntPtr.Zero;

            var ptr = Librdkafka.headers_new((IntPtr)total);
            if (ptr == IntPtr.Zero)
            {
                throw new KafkaException(new Error(ErrorCode.Local_Fail, "Failed to allocate headers list."));
            }

            try
            {
                Span<byte> nameBuffer = stackalloc byte[StackNameBufferSize];
                AddHeadersToHandle(ptr, in first, nameBuffer);
                AddHeadersToHandle(ptr, in second, nameBuffer);
            }
            catch
            {
                Librdkafka.headers_destroy(ptr);
                throw;
            }

            return ptr;
        }

        /// <summary>
        ///     Appends every entry in <paramref name="headers"/> to an existing native
        ///     headers handle. Header names are re-encoded to UTF-8 using
        ///     <paramref name="nameBuffer"/>, falling back to a heap allocation only
        ///     when a name exceeds the buffer. Throws on the first failure; the caller
        ///     owns destroying <paramref name="ptr"/> on throw.
        /// </summary>
        private static unsafe void AddHeadersToHandle(IntPtr ptr, in KafkaHeaders headers, Span<byte> nameBuffer)
        {
            for (int i = 0; i < headers.Count; i++)
            {
                var entry = headers[i];
                if (entry.Name == null)
                {
                    throw new ArgumentNullException(nameof(entry.Name), "Header name must not be null.");
                }

                int nameLen = Encoding.UTF8.GetByteCount(entry.Name);
                Span<byte> nameSpan = nameLen <= nameBuffer.Length
                    ? nameBuffer.Slice(0, nameLen)
                    : new byte[nameLen];

                ReadOnlySpan<byte> valueSpan = entry.Value.Span;

                fixed (char* nameChars = entry.Name)
                fixed (byte* namePtr = nameSpan)
                fixed (byte* valPtr = valueSpan)
                {
                    Encoding.UTF8.GetBytes(nameChars, entry.Name.Length, namePtr, nameLen);

                    var headerErr = Librdkafka.headers_add(
                        ptr,
                        (IntPtr)namePtr, (IntPtr)nameLen,
                        (IntPtr)valPtr, (IntPtr)valueSpan.Length);
                    if (headerErr != ErrorCode.NoError)
                    {
                        throw new KafkaException(new Error(headerErr, "Failed to add header."));
                    }
                }
            }
        }

        /// <inheritdoc/>
        public void RawProduce(string topic, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, IntPtr opaque = default)
            => RawProduce(topic, Partition.Any, key, value, opaque);

        /// <inheritdoc/>
        public unsafe void RawProduce(string topic, Partition partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, IntPtr opaque = default)
        {
            fixed (byte* kp = key)
            fixed (byte* vp = value)
            {
                var err = ((IRawProducer)this).ProduceRawCore(
                    topic, partition,
                    (IntPtr)kp, key.Length,
                    (IntPtr)vp, value.Length,
                    IntPtr.Zero,
                    (IntPtr)MsgFlags.MSG_F_COPY,
                    opaque);
                ThrowIfError(err);
            }
        }

        /// <inheritdoc/>
        public void RawProduce(string topic, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, in KafkaHeaders headers, IntPtr opaque = default)
            => RawProduce(topic, Partition.Any, key, value, in headers, opaque);

        /// <inheritdoc/>
        public unsafe void RawProduce(string topic, Partition partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, in KafkaHeaders headers, IntPtr opaque = default)
        {
            fixed (byte* kp = key)
            fixed (byte* vp = value)
            {
                ((IRawProducer)this).ProduceRawWithHeaders(
                    topic, partition,
                    (IntPtr)kp, key.Length,
                    (IntPtr)vp, value.Length,
                    in headers,
                    (IntPtr)MsgFlags.MSG_F_COPY,
                    opaque);
            }
        }

        /// <inheritdoc/>
        public void RawProduce(string topic, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, in KafkaHeaders headers1, in KafkaHeaders headers2, IntPtr opaque = default)
            => RawProduce(topic, Partition.Any, key, value, in headers1, in headers2, opaque);

        /// <inheritdoc/>
        public unsafe void RawProduce(string topic, Partition partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, in KafkaHeaders headers1, in KafkaHeaders headers2, IntPtr opaque = default)
        {
            fixed (byte* kp = key)
            fixed (byte* vp = value)
            {
                ((IRawProducer)this).ProduceRawWithHeaders(
                    topic, partition,
                    (IntPtr)kp, key.Length,
                    (IntPtr)vp, value.Length,
                    in headers1, in headers2,
                    (IntPtr)MsgFlags.MSG_F_COPY,
                    opaque);
            }
        }

        private void ThrowIfError(ErrorCode err)
        {
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(this.Handle.LibrdkafkaHandle.CreatePossiblyFatalError(err, null));
            }
        }

        /// <summary>
        ///     Overrides the base delivery callback with an allocation-free path:
        ///     wraps the native message pointer in a stack-only
        ///     <see cref="RawDeliveryReport"/> and invokes the user's handler.
        /// </summary>
        protected override unsafe void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            if (this.Handle.LibrdkafkaHandle.IsClosed) return;
            if (rawDeliveryReportHandler == null) return;
            try
            {
                var report = new RawDeliveryReport((rd_kafka_message*)rkmessage);
                rawDeliveryReportHandler(in report);
            }
            catch
            {
                // Swallow user-handler exceptions; librdkafka ABI cannot propagate managed exceptions.
            }
        }
    }
}
