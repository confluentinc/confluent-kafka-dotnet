// Copyright 2015-2016 Andreas Heider
//           2016-2023 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka.Impl
{
    enum RdKafkaType
    {
        Producer,
        Consumer
    }


    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_message
    {
        internal ErrorCode err;                       /* Non-zero for error signaling. */
        internal /* rd_kafka_topic_t * */ IntPtr rkt; /* Topic */
        internal int partition;                       /* Partition */
        internal /* void   * */ IntPtr val;           /* err==0: Message val
                                                       * err!=0: Error string */
        internal UIntPtr  len;                        /* err==0: Message val length
                                                       * err!=0: Error string length */
        internal /* void   * */ IntPtr key;           /* err==0: Optional message key */
        internal UIntPtr  key_len;                    /* err==0: Optional message key length */
        internal long offset;                         /* Consume:
                                                       *   Message offset (or offset for error
                                                       *   if err!=0 if applicable).
                                                       * dr_msg_cb:
                                                       *   Message offset assigned by broker.
                                                       *   If produce.offset.report is set then
                                                       *   each message will have this field set,
                                                       *   otherwise only the last message in
                                                       *   each produced internal batch will
                                                       *   have this field set, otherwise 0. */
        internal /* void  * */ IntPtr _private;       /* Consume:
                                                       *   rdkafka private pointer: DO NOT MODIFY
                                                       * dr_msg_cb:
                                                       *   mgs_opaque from produce() call */
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct rd_kafka_topic_partition
    {
        internal string topic;
        internal int partition;
        internal long offset;
        internal /* void * */ IntPtr metadata;
        internal UIntPtr metadata_size;
        internal /* void * */ IntPtr opaque;
        internal ErrorCode err; /* Error code, depending on use. */
        internal /* void * */ IntPtr _private; /* INTERNAL USE ONLY,
                                                * INITIALIZE TO ZERO, DO NOT TOUCH */
    };

    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_topic_partition_list
    {
        internal int cnt; /* Current number of elements */
        internal int size; /* Allocated size */
        internal /* rd_kafka_topic_partition_t * */ IntPtr elems;
    };

    internal sealed class SafeKafkaHandle : SafeHandleZeroIsInvalid
    {
        private const int RD_KAFKA_PARTITION_UA = -1;

        public volatile IClient owner;

        public RdKafkaType type;

        private object topicHandlesLock = new object();
        private Dictionary<string, SafeTopicHandle> topicHandles
            = new Dictionary<string, SafeTopicHandle>(StringComparer.Ordinal);

        internal SafeTopicHandle newTopic(string topic, IntPtr topicConfigPtr)
        {
            lock (topicHandlesLock)
            {
                if (this.topicHandles.ContainsKey(topic))
                {
                    return topicHandles[topic];
                }

                var topicHandle = this.NewTopic(topic, topicConfigPtr);
                topicHandles.Add(topic, topicHandle);
                return topicHandle;
            }
        }

        public SafeKafkaHandle() : base("kafka") {}

        /// <summary>
        ///     This object is tightly coupled to the referencing Producer /
        ///     Consumer via callback objects passed into the librdkafka
        ///     config. These are not tracked by the CLR, so we need to
        ///     maintain an explicit reference to the containing object here
        ///     so the delegates - which may get called by librdkafka during
        ///     destroy - are guaranteed to exist during finalization.
        ///     Note: objects referenced by this handle (recursively) will 
        ///     not be GC'd at the time of finalization as the freachable
        ///     list is a GC root. Also, the delegates are ok to use since they
        ///     don't have finalizers.
        ///     
        ///     this is a useful reference:
        ///     https://stackoverflow.com/questions/6964270/which-objects-can-i-use-in-a-finalizer-method
        /// </summary>
        internal void SetOwner(IClient owner) { this.owner = owner; }

        public static SafeKafkaHandle Create(RdKafkaType type, IntPtr config, IClient owner)
        {
            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
            var kh = Librdkafka.kafka_new(type, config, errorStringBuilder,(UIntPtr) errorStringBuilder.Capacity);
            if (kh.IsInvalid)
            {
                Librdkafka.conf_destroy(config);
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            kh.SetOwner(owner);
            kh.type = type;
            Library.IncrementKafkaHandleCreateCount();
            return kh;
        }

        /// <summary>
        ///     Prevent AccessViolationException when handle has already been closed.
        ///     Should be called at start of every function using the handle,
        ///     except in ReleaseHandle. 
        /// </summary>
        public void ThrowIfHandleClosed()
        {
            if (IsClosed)
            {
                throw new ObjectDisposedException($"handle is destroyed", innerException: null);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var kv in topicHandles)
                {
                    kv.Value.Dispose();
                }
            }

            base.Dispose(disposing);
        }

        protected override bool ReleaseHandle()
        {
            Library.IncrementKafkaHandleDestroyCount();

            // Librdkafka.destroy / Librdkafka.destroy_flags is a static
            // object which means at this point we can be sure it hasn't
            // already been GC'd.

            if (type == RdKafkaType.Producer)
            {
                Librdkafka.destroy(handle);
            }
            else
            {
                Librdkafka.destroy_flags(handle, (IntPtr)Librdkafka.DestroyFlags.RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
            }

            return true;
        }

        internal Error CreatePossiblyFatalMessageError(IntPtr msgPtr)
        {
            var msg = Util.Marshal.PtrToStructure<rd_kafka_message>(msgPtr);
            if (msg.err == ErrorCode.Local_Fatal)
            {
                return CreateFatalError();
            }
            return new Error(msg.err, Util.Marshal.PtrToStringUTF8(Librdkafka.message_errstr(msgPtr)));
        }

        internal Error CreatePossiblyFatalError(ErrorCode err, string reason)
        {
            if (err == ErrorCode.Local_Fatal)
            {
                return CreateFatalError();
            }
            return new Error(err, reason);
        }

        internal Error CreateFatalError()
        {
            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
            var err = Librdkafka.fatal_error(this.handle, errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
            return new Error(err, errorStringBuilder.ToString(), true);
        }

        private string name;
        internal string Name
        {
            get
            {
                if (name == null)
                {
                    ThrowIfHandleClosed();
                    name = Util.Marshal.PtrToStringUTF8(Librdkafka.name(handle));
                }
                return name;
            }
        }

        private int OutQueueLength
        {
            get
            {
                ThrowIfHandleClosed();
                return Librdkafka.outq_len(handle);
            }
        }

        internal int Flush(int millisecondsTimeout)
        {
            ThrowIfHandleClosed();
            Librdkafka.flush(handle, new IntPtr(millisecondsTimeout));
            return OutQueueLength;
        }

        internal int AddBrokers(string brokers)
        {
            ThrowIfHandleClosed();
            if (brokers == null)
            {
                throw new ArgumentNullException("brokers", "Argument 'brokers' must not be null.");
            }
            return (int)Librdkafka.brokers_add(handle, brokers);
        }

        internal void SetSaslCredentials(string username, string password)
        {
            ThrowIfHandleClosed();
            IntPtr err = Librdkafka.sasl_set_credentials(handle, username, password);
            if (err != IntPtr.Zero)
            {
                throw new KafkaException(new Error(err, true));
            }
        }

        internal int Poll(IntPtr millisecondsTimeout)
        {
            ThrowIfHandleClosed();
            return (int)Librdkafka.poll(handle, millisecondsTimeout);
        }

        /// <summary>
        ///     Setting the config parameter to IntPtr.Zero returns the handle of an 
        ///     existing topic, or an invalid handle if a topic with name <paramref name="topic" /> 
        ///     does not exist. Note: Only the first applied configuration for a specific
        ///     topic will be used.
        /// </summary>
        internal SafeTopicHandle NewTopic(string topic, IntPtr config)
        {
            ThrowIfHandleClosed();

            // Increase the refcount to this handle to keep it alive for
            // at least as long as the topic handle.
            // Corresponding decrement happens in the topic handle ReleaseHandle method.
            bool success = false;
            DangerousAddRef(ref success);
            if (!success)
            {
                Librdkafka.topic_conf_destroy(config);
                throw new Exception("Failed to create topic (DangerousAddRef failed)");
            }

            SafeTopicHandle topicHandle = null;
            using (var pinnedString = new Util.Marshal.StringAsPinnedUTF8(topic))
            {
                topicHandle = Librdkafka.topic_new(handle, pinnedString.Ptr, config);
            }

            if (topicHandle.IsInvalid)
            {
                DangerousRelease();
                throw new KafkaException(CreatePossiblyFatalError(Librdkafka.last_error(), null));
            }

            topicHandle.kafkaHandle = this;
            return topicHandle;
        }

        private IntPtr marshalHeaders(IReadOnlyList<IHeader> headers)
        {
            var headersPtr = IntPtr.Zero;

            if (headers != null && headers.Count > 0)
            {
                headersPtr = Librdkafka.headers_new((IntPtr)headers.Count);
                if (headersPtr == IntPtr.Zero)
                {
                    throw new Exception("Failed to create headers list.");
                }
                for (int x = 0; x < headers.Count; x++)
                {
                    var header = headers[x];

                    if (header.Key == null)
                    {
                        throw new ArgumentNullException("Message header keys must not be null.");
                    }
                    byte[] keyBytes = System.Text.UTF8Encoding.UTF8.GetBytes(header.Key);
                    GCHandle pinnedKey = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                    IntPtr keyPtr = pinnedKey.AddrOfPinnedObject();
                    IntPtr valuePtr = IntPtr.Zero;
                    GCHandle pinnedValue = default(GCHandle);
                    if (header.GetValueBytes() != null)
                    {
                        pinnedValue = GCHandle.Alloc(header.GetValueBytes(), GCHandleType.Pinned);
                        valuePtr = pinnedValue.AddrOfPinnedObject();
                    }
                    ErrorCode err = Librdkafka.headers_add(headersPtr, keyPtr, (IntPtr)keyBytes.Length, valuePtr, (IntPtr)(header.GetValueBytes() == null ? 0 : header.GetValueBytes().Length));
                    // copies of key and value have been made in headers_list_add - pinned values are no longer referenced.
                    pinnedKey.Free();
                    if (header.GetValueBytes() != null)
                    {
                        pinnedValue.Free();
                    }
                    if (err != ErrorCode.NoError)
                    {
                        throw new KafkaException(CreatePossiblyFatalError(err, null));
                    }
                }
            }

            return headersPtr;
        }

        internal ErrorCode Produce(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            int partition,
            long timestamp,
            IReadOnlyList<IHeader> headers,
            IntPtr opaque)
        {
            var pValue = IntPtr.Zero;
            var pKey = IntPtr.Zero;

            var gchValue = default(GCHandle);
            var gchKey = default(GCHandle);

            if (val == null)
            {
                if (valOffset != 0 || valLength != 0)
                {
                    throw new ArgumentException("valOffset and valLength parameters must be 0 when producing null values.");
                }
            }
            else
            {
                gchValue = GCHandle.Alloc(val, GCHandleType.Pinned);
                pValue = Marshal.UnsafeAddrOfPinnedArrayElement(val, valOffset);
            }

            if (key == null)
            {
                if (keyOffset != 0 || keyLength != 0)
                {
                    throw new ArgumentException("keyOffset and keyLength parameters must be 0 when producing null key values.");
                }
            }
            else
            {
                gchKey = GCHandle.Alloc(key, GCHandleType.Pinned);
                pKey = Marshal.UnsafeAddrOfPinnedArrayElement(key, keyOffset);
            }

            IntPtr headersPtr = marshalHeaders(headers);

            try
            {
                var errorCode = Librdkafka.produceva(
                    handle,
                    topic,
                    partition,
                    (IntPtr)MsgFlags.MSG_F_COPY,
                    pValue, (UIntPtr)valLength,
                    pKey, (UIntPtr)keyLength,
                    timestamp,
                    headersPtr,
                    opaque);

                if (errorCode != ErrorCode.NoError)
                {
                    if (headersPtr != IntPtr.Zero)
                    {
                        Librdkafka.headers_destroy(headersPtr);
                    }
                }

                return errorCode;
            }
            catch
            {
                if (headersPtr != IntPtr.Zero)
                {
                    Librdkafka.headers_destroy(headersPtr);
                }
                throw;
            }
            finally
            {
                if (val != null)
                {
                    gchValue.Free();
                }

                if (key != null)
                {
                    gchKey.Free();
                }
            }
        }

        private static int[] MarshalCopy(IntPtr source, int length)
        {
            int[] res = new int[length];
            Marshal.Copy(source, res, 0, length);
            return res;
        }

        /// <summary>
        ///     - allTopics=true - request all topics from cluster
        ///     - allTopics=false, topic=null - request only locally known topics (topic_new():ed topics or otherwise locally referenced once, such as consumed topics)
        ///     - allTopics=false, topic=valid - request specific topic
        /// </summary>
        internal Metadata GetMetadata(bool allTopics, SafeTopicHandle topic, int millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.metadata(
                handle, allTopics,
                topic?.DangerousGetHandle() ?? IntPtr.Zero,
                /* const struct rd_kafka_metadata ** */ out IntPtr metaPtr,
                (IntPtr)millisecondsTimeout);

            if (err == ErrorCode.NoError)
            {
                try {
                    var meta = Util.Marshal.PtrToStructure<rd_kafka_metadata>(metaPtr);

                    var brokers = Enumerable.Range(0, meta.broker_cnt)
                        .Select(i => Util.Marshal.PtrToStructure<rd_kafka_metadata_broker>(
                                    meta.brokers + i * Util.Marshal.SizeOf<rd_kafka_metadata_broker>()))
                        .Select(b => new BrokerMetadata(b.id, b.host, b.port))
                        .ToList();

                    var topics = Enumerable.Range(0, meta.topic_cnt)
                        .Select(i => Util.Marshal.PtrToStructure<rd_kafka_metadata_topic>(
                                    meta.topics + i * Util.Marshal.SizeOf<rd_kafka_metadata_topic>()))
                        .Select(t => new TopicMetadata(
                                t.topic,
                                Enumerable.Range(0, t.partition_cnt)
                                    .Select(j => Util.Marshal.PtrToStructure<rd_kafka_metadata_partition>(
                                                t.partitions + j * Util.Marshal.SizeOf<rd_kafka_metadata_partition>()))
                                    .Select(p => new PartitionMetadata(
                                            p.id,
                                            p.leader,
                                            MarshalCopy(p.replicas, p.replica_cnt),
                                            MarshalCopy(p.isrs, p.isr_cnt),
                                            p.err
                                        ))
                                    .ToList(),
                                t.err
                            ))
                        .ToList();

                    return new Metadata(
                        brokers,
                        topics,
                        meta.orig_broker_id,
                        meta.orig_broker_name
                    );
                }
                finally
                {
                    Librdkafka.metadata_destroy(metaPtr);
                }
            }
            else
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
        }

        internal ErrorCode PollSetConsumer()
        {
            ThrowIfHandleClosed();
            return Librdkafka.poll_set_consumer(handle);
        }

        internal void InitTransactions(int millisecondsTimeout)
        {
            var error = new Error(Librdkafka.init_transactions(this.handle, (IntPtr)millisecondsTimeout));
            if (error.Code != ErrorCode.NoError)
            {
                if (error.IsRetriable)
                {
                    throw new KafkaRetriableException(error);
                }
                throw new KafkaException(error);
            }
        }

        internal void BeginTransaction()
        {
            var error = new Error(Librdkafka.begin_transaction(this.handle));
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }
        }

        internal void CommitTransaction(int millisecondsTimeout)
        {
            var error = new Error(Librdkafka.commit_transaction(this.handle, (IntPtr)millisecondsTimeout));
            if (error.Code != ErrorCode.NoError)
            {
                if (error.TxnRequiresAbort)
                {
                    throw new KafkaTxnRequiresAbortException(error);
                }
                if (error.IsRetriable)
                {
                    throw new KafkaRetriableException(error);
                }
                throw new KafkaException(error);
            }
        }

        internal void AbortTransaction(int millisecondsTimeout)
        {
            var error = new Error(Librdkafka.abort_transaction(this.handle, (IntPtr)millisecondsTimeout));
            if (error.Code != ErrorCode.NoError)
            {
                if (error.IsRetriable)
                {
                    throw new KafkaRetriableException(error);
                }
                throw new KafkaException(error);
            }
        }

        internal void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, int millisecondsTimeout)
        {
            if (!(groupMetadata is ConsumerGroupMetadata))
            {
                throw new ArgumentException("groupMetadata object must be a value acquired via Consumer.ConsumerGroupMetadata.");
            }
            var serializedMetadata = ((ConsumerGroupMetadata)groupMetadata).serializedMetadata;

            var cgmdPtr = IntPtr.Zero;
            var offsetsPtr = IntPtr.Zero;
            try
            {
                cgmdPtr = this.DeserializeConsumerGroupMetadata(serializedMetadata);
                offsetsPtr = GetCTopicPartitionList(offsets);

                var error = new Error(Librdkafka.send_offsets_to_transaction(this.handle, offsetsPtr, cgmdPtr, (IntPtr)millisecondsTimeout));
                if (error.Code != ErrorCode.NoError)
                {
                    if (error.IsRetriable)
                    {
                        throw new KafkaRetriableException(error);
                    }
                    if (error.TxnRequiresAbort)
                    {
                        throw new KafkaTxnRequiresAbortException(error);
                    }
                    throw new KafkaException(error);
                }
            }
            finally
            {
                if (offsetsPtr != IntPtr.Zero)
                {
                    Librdkafka.topic_partition_list_destroy(offsetsPtr);
                }
                if (cgmdPtr != IntPtr.Zero)
                {
                    this.DestroyConsumerGroupMetadata(cgmdPtr);
                }
            }
        }

        internal IntPtr GetConsumerGroupMetadata()
            => Librdkafka.consumer_group_metadata(this.handle);

        internal void DestroyConsumerGroupMetadata(IntPtr consumerGroupMetadata)
            => Librdkafka.consumer_group_metadata_destroy(consumerGroupMetadata);

        internal unsafe byte[] SerializeConsumerGroupMetadata(IntPtr consumerGroupMetadata)
        {
            var error = new Error(Librdkafka.consumer_group_metadata_write(consumerGroupMetadata, out IntPtr buffer, out IntPtr dataSize));
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }
            var result = new byte[(int)dataSize];
            byte* pIter = (byte*)buffer;
            for (int i=0; i<(int)dataSize; ++i)
            {
                result[i] = *pIter++;
            }
            Librdkafka.mem_free(IntPtr.Zero, buffer);
            return result;
        }

        internal IntPtr DeserializeConsumerGroupMetadata(byte[] buffer)
        {
            var error = new Error(Librdkafka.consumer_group_metadata_read(out IntPtr cgmd, buffer, (IntPtr)buffer.Length));
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }
            return cgmd;
        }

        internal WatermarkOffsets QueryWatermarkOffsets(string topic, int partition, int millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.query_watermark_offsets(handle, topic, partition, out long low, out long high, (IntPtr)millisecondsTimeout);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            return new WatermarkOffsets(low,  high);
        }

        internal WatermarkOffsets GetWatermarkOffsets(string topic, int partition)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.get_watermark_offsets(handle, topic, partition, out long low, out long high);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            return new WatermarkOffsets(low, high);
        }

        internal List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, int millisecondsTimeout)
        {
            if (timestampsToSearch.Count() == 0)
            {
                // librdkafka returns invalid argument in this case (which would result in an exception).
                // an empty result is a better response (also matches the java api).
                return new List<TopicPartitionOffset>();
            }

            var offsets = timestampsToSearch.Select(t => new TopicPartitionOffset(t.TopicPartition, t.Timestamp.UnixTimestampMs)).ToList();
            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            try
            {
                // The timestamps to query are represented as Offset property in offsets param on input, 
                // and Offset property will contain the offset on output
                var errorCode = Librdkafka.offsets_for_times(handle, cOffsets, (IntPtr) millisecondsTimeout);
                if (errorCode != ErrorCode.NoError)
                {
                    throw new KafkaException(CreatePossiblyFatalError(errorCode, null));
                }

                var result = GetTopicPartitionOffsetErrorList(cOffsets);

                if (result.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
                {
                    throw new TopicPartitionOffsetException(result);
                }

                return result.Select(r => r.TopicPartitionOffset).ToList();
            }
            finally
            {
                Librdkafka.topic_partition_list_destroy(cOffsets);
            }
        }

        internal void Subscribe(IEnumerable<string> topics)
        {
            ThrowIfHandleClosed();

            if (topics == null)
            {
                throw new ArgumentNullException("Subscription must not be null");
            }

            if (topics.Any(t => t == null))
            {
                throw new ArgumentNullException("Subscribed-to topic must not be null");
            }

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) topics.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }
            foreach (string topic in topics)
            {
                Librdkafka.topic_partition_list_add(list, topic, RD_KAFKA_PARTITION_UA);
            }

            ErrorCode err = Librdkafka.subscribe(handle, list);
            Librdkafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
        }

        internal void Unsubscribe()
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.unsubscribe(handle);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
        }

        internal IntPtr ConsumerPoll(IntPtr millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            // TODO: There is a newer librdkafka interface for this now. Use that.
            return Librdkafka.consumer_poll(handle, millisecondsTimeout);
        }

        internal void ConsumerClose()
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.consumer_close(handle);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
        }

        internal List<TopicPartition> GetAssignment()
        {
            ThrowIfHandleClosed();

            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = Librdkafka.assignment(handle, out listPtr);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            var ret = GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.TopicPartition).ToList();
            Librdkafka.topic_partition_list_destroy(listPtr);
            return ret;
        }

        internal List<string> GetSubscription()
        {
            ThrowIfHandleClosed();

            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = Librdkafka.subscription(handle, out listPtr);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
            var ret = GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.Topic).ToList();
            Librdkafka.topic_partition_list_destroy(listPtr);
            return ret;
        }

        private void AssignImpl(IEnumerable<TopicPartitionOffset> partitions,
                                Func<IntPtr, IntPtr, ErrorCode> assignMethodErr,
                                Func<IntPtr, IntPtr, IntPtr> assignMethodError)
        {
            ThrowIfHandleClosed();

            IntPtr list = IntPtr.Zero;
            if (partitions != null)
            {
                list = Librdkafka.topic_partition_list_new((IntPtr) partitions.Count());
                if (list == IntPtr.Zero)
                {
                    throw new Exception("Failed to create topic partition list");
                }
                foreach (var partition in partitions)
                {
                    if (partition.Topic == null)
                    {
                        Librdkafka.topic_partition_list_destroy(list);
                        throw new ArgumentException("Partition topic must not be null");
                    }

                    IntPtr ptr = Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
                    Marshal.WriteInt64(
                        ptr,
                        (int) Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                        partition.Offset);
                }
            }

            ErrorCode err = ErrorCode.NoError;
            Error error = null;

            if (assignMethodErr != null)
            {
                err = assignMethodErr(handle, list);
            }
            else if (assignMethodError != null)
            {
                var errorPtr = assignMethodError(handle, list);
                if (errorPtr != IntPtr.Zero)
                {
                    error = new Error(errorPtr);
                }
            }

            if (list != IntPtr.Zero)
            {
                Librdkafka.topic_partition_list_destroy(list);
            }

            if (err != ErrorCode.NoError) { throw new KafkaException(CreatePossiblyFatalError(err, null)); }
            else if (error != null) { throw new KafkaException(error); }
        }

        internal void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => AssignImpl(partitions, Librdkafka.assign, null);

        internal void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
            => AssignImpl(partitions, null, Librdkafka.incremental_assign);

        internal void IncrementalUnassign(IEnumerable<TopicPartitionOffset> partitions)
            => AssignImpl(partitions, null, Librdkafka.incremental_unassign);

        internal bool AssignmentLost
        {
            get
            {
                ThrowIfHandleClosed();
                return (Librdkafka.assignment_lost(handle) != IntPtr.Zero);
            }
        }

        internal string RebalanceProtocol
        {
            get
            {
                ThrowIfHandleClosed();
                var rebalanceProtocolPtr = Librdkafka.rebalance_protocol(handle);
                if (rebalanceProtocolPtr == IntPtr.Zero) {
                    return null;
                }
                return Util.Marshal.PtrToStringUTF8(rebalanceProtocolPtr);
            }
        }

        internal void StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            ThrowIfHandleClosed();

            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            ErrorCode err = Librdkafka.offsets_store(handle, cOffsets);
            var results = GetTopicPartitionOffsetErrorList(cOffsets);
            Librdkafka.topic_partition_list_destroy(cOffsets);

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            if (results.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(results);
            }
        }

        /// <summary>
        ///     Dummy commit callback that does nothing but prohibits
        ///     triggering the global offset_commit_cb.
        ///     Used by manual commits.
        /// </summary>
        static void DummyOffsetCommitCb(IntPtr rk, ErrorCode err, IntPtr offsets, IntPtr opaque)
        {
            return;
        }


        internal List<TopicPartitionOffset> Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            ThrowIfHandleClosed();

            if (offsets != null && offsets.Count() == 0)
            {
                return new List<TopicPartitionOffset>();
            }

            // Create temporary queue so we can get the offset commit results
            // as an event instead of a callback.
            // We still need to specify a dummy callback (that does nothing)
            // to prevent the global offset_commit_cb to kick in.
            IntPtr cQueue = Librdkafka.queue_new(handle);
            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            ErrorCode err = Librdkafka.commit_queue(handle, cOffsets, cQueue, DummyOffsetCommitCb, IntPtr.Zero);
            if (cOffsets != IntPtr.Zero)
            {
                Librdkafka.topic_partition_list_destroy(cOffsets);
            }
            if (err != ErrorCode.NoError)
            {
                Librdkafka.queue_destroy(cQueue);
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            // Wait for commit to finish
            IntPtr rkev = Librdkafka.queue_poll(cQueue, -1/*infinite*/);
            Librdkafka.queue_destroy(cQueue);
            if (rkev == IntPtr.Zero)
            {
                // This shouldn't happen since timeout is infinite
                throw new KafkaException(ErrorCode.Local_TimedOut);
            }

            var errorCode = Librdkafka.event_error(rkev);
            if (errorCode != ErrorCode.NoError)
            {
                var errorString = Librdkafka.event_error_string(rkev);
                Librdkafka.event_destroy(rkev);
                throw new KafkaException(CreatePossiblyFatalError(errorCode, errorString));
            }

            var result = GetTopicPartitionOffsetErrorList(Librdkafka.event_topic_partition_list(rkev));
            Librdkafka.event_destroy(rkev);

            if (result.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(result);
            }

            return result.Select(r => r.TopicPartitionOffset).ToList();
        }

        internal void Seek(string topic, Partition partition, Offset offset, int millisecondsTimeout,
                           int? leaderEpoch = null)
        {
            ThrowIfHandleClosed();
          
            ErrorCode result;
            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) 1);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create seek partition list");
            }

            IntPtr listPartition =
                Librdkafka.topic_partition_list_add(list, topic, partition);

            Marshal.WriteInt64(
                listPartition,
                (int) Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                offset);
            
            if (leaderEpoch != null)
            {
                Librdkafka.topic_partition_set_leader_epoch(listPartition,
                                                            leaderEpoch.Value);
            }

            IntPtr resultError = Librdkafka.seek_partitions(
                handle,
                list, (IntPtr)millisecondsTimeout);
            
            if (resultError != IntPtr.Zero)
            {
                result = Librdkafka.error_code(resultError);
            }
            else
            {
                result = ErrorCode.NoError;
            }
            
            if (result == ErrorCode.NoError)
            {
                var topicPartitionErrors = GetTopicPartitionErrorList(list);
                foreach (var tp in topicPartitionErrors)
                {
                    if (tp.Error != ErrorCode.NoError)
                    {
                        result = tp.Error;
                    }
                }
            }
            
            Librdkafka.topic_partition_list_destroy(list);

            if (result != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(result, null));
            }
        }

        internal List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
        {
            ThrowIfHandleClosed();

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) partitions.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create pause partition list");
            }

            foreach (var partition in partitions)
            {
                Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);

            }
            ErrorCode err = Librdkafka.pause_partitions(handle, list);
            var result = GetTopicPartitionErrorList(list);
            Librdkafka.topic_partition_list_destroy(list);

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            if (result.Where(tpe => tpe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionException(result);
            }

            return result;
        }

        internal List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
        {
            ThrowIfHandleClosed();

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) partitions.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create resume partition list");
            }

            foreach (var partition in partitions)
            {
                Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
            }

            ErrorCode err = Librdkafka.resume_partitions(handle, list);
            var result = GetTopicPartitionErrorList(list);
            Librdkafka.topic_partition_list_destroy(list);

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            if (result.Where(tpe => tpe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionException(result);
            }

            return result;
        }


        internal List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, IntPtr timeout_ms)
        {
            ThrowIfHandleClosed();

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) partitions.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create committed partition list");
            }
            foreach (var partition in partitions)
            {
                Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
            }

            ErrorCode err = Librdkafka.committed(handle, list, timeout_ms);
            var result = GetTopicPartitionOffsetErrorList(list);
            Librdkafka.topic_partition_list_destroy(list);

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            if (result.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(result);
            }

            return result.Select(r => r.TopicPartitionOffset).ToList();
        }


        internal List<TopicPartitionOffset> Position(IEnumerable<TopicPartition> partitions)
        {
            ThrowIfHandleClosed();

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr) partitions.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create position list");
            }
            foreach (var partition in partitions)
            {
                Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
            }

            ErrorCode err = Librdkafka.position(handle, list);
            var result = GetTopicPartitionOffsetErrorList(list);
            Librdkafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }

            if (result.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(result);
            }

            return result.Select(r => r.TopicPartitionOffset).ToList();
        }

        internal string MemberId
        {
            get
            {
                ThrowIfHandleClosed();

                IntPtr strPtr = Librdkafka.memberid(handle);
                if (strPtr == IntPtr.Zero)
                {
                    return null;
                }

                string memberId = Util.Marshal.PtrToStringUTF8(strPtr);
                Librdkafka.mem_free(handle, strPtr);
                return memberId;
            }
        }

        internal static List<TopicPartitionError> GetTopicPartitionErrorList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                throw new InvalidOperationException("FATAL: Cannot marshal from a NULL ptr.");
            }

            var list = Util.Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Util.Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Util.Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartitionError(ktp.topic, ktp.partition, ktp.err))
                .ToList();
        }

        internal static List<TopicPartitionOffsetError> GetTopicPartitionOffsetErrorList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                throw new InvalidOperationException("FATAL: Cannot marshal from a NULL ptr.");
            }

            var list = Util.Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            var returnList = new List<TopicPartitionOffsetError>(list.cnt);
            for (var i = 0; i < list.cnt; i++)
            {
                var ptr = list.elems + i * Util.Marshal.SizeOf<rd_kafka_topic_partition>();
                var ktp = Util.Marshal.PtrToStructure<rd_kafka_topic_partition>(ptr);
                returnList.Add(new TopicPartitionOffsetError(
                    ktp.topic,
                    ktp.partition,
                    ktp.offset,
                    ktp.err,
                    Librdkafka.topic_partition_get_leader_epoch(ptr)
                ));
            }
            return returnList;
        }

        /// <summary>
        ///     Creates and returns a List{TopicPartition} from a C rd_kafka_topic_partition_list_t *.
        /// </summary>
        internal static List<TopicPartition> GetTopicPartitionList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                throw new InvalidOperationException("FATAL: Cannot marshal from a NULL ptr.");
            }

            var list = Util.Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Util.Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Util.Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartition(
                        ktp.topic,
                        ktp.partition
                    ))
                .ToList();
        }

        /// <summary>
        ///     Creates and returns a C rd_kafka_topic_partition_list_t * populated by offsets.
        /// </summary>
        /// <returns>
        ///     If offsets is null a null IntPtr will be returned, else a IntPtr
        ///     which must destroyed with LibRdKafka.topic_partition_list_destroy()
        /// </returns>
        internal static IntPtr GetCTopicPartitionList(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (offsets == null)
            {
                return IntPtr.Zero;
            }

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr)offsets.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }

            foreach (var p in offsets)
            {
                if (p.Topic == null)
                {
                    Librdkafka.topic_partition_list_destroy(list);
                    throw new ArgumentException("Cannot create offsets list because one or more topics is null.");
                }
                IntPtr ptr = Librdkafka.topic_partition_list_add(list, p.Topic, p.Partition);
                Marshal.WriteInt64(ptr, (int)Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"), p.Offset);
                
                if (p.LeaderEpoch != null)
                {
                    Librdkafka.topic_partition_set_leader_epoch(ptr,
                                                                p.LeaderEpoch.Value);
                }
            }

            return list;
        }

        /// <summary>
        ///     Creates and returns a C rd_kafka_topic_partition_list_t *.
        /// </summary>
        /// <returns>
        ///     If offsets is null a null IntPtr will be returned, else a IntPtr
        ///     which must destroyed with LibRdKafka.topic_partition_list_destroy()
        /// </returns>
        internal static IntPtr GetCTopicPartitionList(IEnumerable<TopicPartition> partitions)
        {
            if (partitions == null)
            {
                return IntPtr.Zero;
            }

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr)partitions.Count());
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }

            foreach (var p in partitions)
            {
                if (p.Topic == null)
                {
                    Librdkafka.topic_partition_list_destroy(list);
                    throw new ArgumentException("Cannot create offsets list because one or more topics is null.");
                }
                Librdkafka.topic_partition_list_add(list, p.Topic, p.Partition);
            }

            return list;
        }

        static byte[] CopyBytes(IntPtr ptr, IntPtr len)
        {
            byte[] data = null;
            if (ptr != IntPtr.Zero)
            {
                data = new byte[(int) len];
                Marshal.Copy(ptr, data, 0, (int) len);
            }
            return data;
        }

        internal GroupInfo ListGroup(string group, int millisecondsTimeout)
        {
            if (group == null)
            {
                throw new ArgumentNullException("group", "Argument 'group' must not be null.");
            }

            return ListGroupsImpl(group, millisecondsTimeout).FirstOrDefault();
        }

        internal List<GroupInfo> ListGroups(int millisecondsTimeout)
            => ListGroupsImpl(null, millisecondsTimeout);

        private List<GroupInfo> ListGroupsImpl(string group, int millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.list_groups(handle, group, out IntPtr grplistPtr, (IntPtr)millisecondsTimeout);
            if (err == ErrorCode.NoError)
            {
                var list = Util.Marshal.PtrToStructure<rd_kafka_group_list>(grplistPtr);
                var groups = Enumerable.Range(0, list.group_cnt)
                    .Select(i => Util.Marshal.PtrToStructure<rd_kafka_group_info>(
                        list.groups + i * Util.Marshal.SizeOf<rd_kafka_group_info>()))
                    .Select(gi => new GroupInfo(
                            new BrokerMetadata(
                                gi.broker.id,
                                gi.broker.host,
                                gi.broker.port
                            ),
                            gi.group,
                            gi.err,
                            gi.state,
                            gi.protocol_type,
                            gi.protocol,
                            Enumerable.Range(0, gi.member_cnt)
                                .Select(j => Util.Marshal.PtrToStructure<rd_kafka_group_member_info>(
                                    gi.members + j * Util.Marshal.SizeOf<rd_kafka_group_member_info>()))
                                .Select(mi => new GroupMemberInfo(
                                        mi.member_id,
                                        mi.client_id,
                                        mi.client_host,
                                        CopyBytes(
                                            mi.member_metadata,
                                            mi.member_metadata_size),
                                        CopyBytes(
                                            mi.member_assignment,
                                            mi.member_assignment_size)
                                    ))
                                .ToList()
                        ))
                    .ToList();
                Librdkafka.group_list_destroy(grplistPtr);
                return groups;
            }
            else
            {
                throw new KafkaException(CreatePossiblyFatalError(err, null));
            }
        }


        internal IntPtr CreateQueue()
        {
            return Librdkafka.queue_new(handle);
        }

        internal void DestroyQueue(IntPtr queue)
        {
            Librdkafka.queue_destroy(queue);
        }

        internal IntPtr QueuePoll(IntPtr queue, int millisecondsTimeout)
        {
            return Librdkafka.queue_poll(queue, millisecondsTimeout);
        }


        //
        // Admin Client
        //

        private void setOption_ValidatOnly(IntPtr optionsPtr, bool validateOnly)
        {
            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
            var errorCode = Librdkafka.AdminOptions_set_validate_only(optionsPtr, (IntPtr)(validateOnly ? 1 : 0), errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
            if (errorCode != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
            }
        }

        private void setOption_RequestTimeout(IntPtr optionsPtr, TimeSpan? timeout)
        {
            if (timeout != null)
            {
                var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
                var errorCode = Librdkafka.AdminOptions_set_request_timeout(optionsPtr, (IntPtr)(int)(timeout.Value.TotalMilliseconds), errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                if (errorCode != ErrorCode.NoError)
                {
                    throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
                }
            }
        }

        private void setOption_OperationTimeout(IntPtr optionsPtr, TimeSpan? timeout)
        {
            if (timeout != null)
            {
                var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
                var errorCode = Librdkafka.AdminOptions_set_operation_timeout(optionsPtr, (IntPtr)(int)(timeout.Value.TotalMilliseconds), errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                if (errorCode != ErrorCode.NoError)
                {
                    throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
                }
            }
        }

        private void setOption_RequireStableOffsets(IntPtr optionsPtr, bool requireStable)
        {
            var rError = Librdkafka.AdminOptions_set_require_stable_offsets(optionsPtr, (IntPtr)(int)(requireStable ? 1 : 0));
            var error = new Error(rError, true);
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }

        }

        private void setOption_IncludeAuthorizedOperations(IntPtr optionsPtr, bool includeAuthorizedOperations)
        {
            var rError = Librdkafka.AdminOptions_set_include_authorized_operations(optionsPtr, (IntPtr)(int)(includeAuthorizedOperations ? 1 : 0));
            var error = new Error(rError, true);
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }

        }

        private void setOption_MatchConsumerGroupStates(IntPtr optionsPtr, ConsumerGroupState[] states)
        {
            var error = Librdkafka.AdminOptions_set_match_consumer_group_states(optionsPtr, states, (UIntPtr)states.Count());
            if (error != IntPtr.Zero)
            {
                throw new KafkaException(new Error(error, true));
            }
        }

        private void setOption_MatchConsumerGroupTypes(IntPtr optionsPtr, ConsumerGroupType[] types)
        {
            var error = Librdkafka.AdminOptions_set_match_consumer_group_types(optionsPtr, types, (UIntPtr)types.Count());
            if (error != IntPtr.Zero)
            {
                throw new KafkaException(new Error(error, true));
            }
        }

        private void setOption_IsolationLevel(IntPtr optionsPtr, IsolationLevel IsolationLevel)
        {
            var rError = Librdkafka.AdminOptions_set_isolation_level(optionsPtr, (IntPtr)(int)IsolationLevel);
            var error = new Error(rError, true);
            if (error.Code != ErrorCode.NoError)
            {
                throw new KafkaException(error);
            }
        }

        private void setOption_completionSource(IntPtr optionsPtr, IntPtr completionSourcePtr)
            => Librdkafka.AdminOptions_set_opaque(optionsPtr, completionSourcePtr);


        internal void AlterConfigs(
            IDictionary<ConfigResource, List<ConfigEntry>> configs,
            AlterConfigsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new AlterConfigsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.AlterConfigs);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] configPtrs = new IntPtr[configs.Count()];
            int configPtrsIdx = 0;
            foreach (var config in configs)
            {
                var resource = config.Key;
                var resourceConfig = config.Value;

                if (string.IsNullOrEmpty(resource.Name))
                {
                    throw new ArgumentException("Resource must be specified.");
                }
                var resourcePtr = Librdkafka.ConfigResource_new(resource.Type, resource.Name);
                foreach (var rc in resourceConfig)
                {
                    if (string.IsNullOrEmpty(rc.Name))
                    {
                        throw new ArgumentException($"config name must be specified for {resource}");
                    }

                    var errorCode = Librdkafka.ConfigResource_set_config(resourcePtr, rc.Name, rc.Value);

                    if (errorCode != ErrorCode.NoError)
                    {
                        throw new KafkaException(CreatePossiblyFatalError(errorCode, null));
                    }
                }
                configPtrs[configPtrsIdx++] = resourcePtr;
            }

            Librdkafka.AlterConfigs(handle, configPtrs, (UIntPtr)configPtrs.Length, optionsPtr, resultQueuePtr);

            for (int i=0; i<configPtrs.Length; ++i)
            {
                Librdkafka.ConfigResource_destroy(configPtrs[i]);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void IncrementalAlterConfigs(
            IDictionary<ConfigResource, List<ConfigEntry>> configs,
            IncrementalAlterConfigsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            IntPtr[] configPtrs = new IntPtr[configs.Count()];
            int configPtrsIdx = 0;
            foreach (var config in configs)
            {
                var resource = config.Key;
                var resourceConfig = config.Value;

                if (string.IsNullOrEmpty(resource.Name))
                {
                    throw new ArgumentException("Resource must be specified.");
                }

                var resourcePtr = Librdkafka.ConfigResource_new(resource.Type, resource.Name);
                foreach (var rc in resourceConfig)
                {
                    if (string.IsNullOrEmpty(rc.Name))
                    {
                        throw new ArgumentException($"Config name must be specified for {resource}");
                    }
                    
                    var error = Librdkafka.ConfigResource_add_incremental_config(resourcePtr, rc.Name, rc.IncrementalOperation, rc.Value);
                    if (error != IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(error, true));
                    }
                }
                configPtrs[configPtrsIdx++] = resourcePtr;
            }

            options = options ?? new IncrementalAlterConfigsOptions();
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.IncrementalAlterConfigs);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);
            
            Librdkafka.IncrementalAlterConfigs(handle, configPtrs, (UIntPtr)configPtrs.Length, optionsPtr, resultQueuePtr);

            for (int i=0; i<configPtrs.Length; ++i)
            {
                Librdkafka.ConfigResource_destroy(configPtrs[i]);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void DescribeConfigs(
            IEnumerable<ConfigResource> resources,
            DescribeConfigsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DescribeConfigsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeConfigs);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] configPtrs = new IntPtr[resources.Count()];
            int configPtrsIdx = 0;
            foreach (var resource in resources)
            {
                if (string.IsNullOrEmpty(resource.Name))
                {
                    throw new ArgumentException("Resource must be specified.");
                }
                var resourcePtr = Librdkafka.ConfigResource_new(resource.Type, resource.Name);
                configPtrs[configPtrsIdx++] = resourcePtr;
            }

            Librdkafka.DescribeConfigs(handle, configPtrs, (UIntPtr)configPtrs.Length, optionsPtr, resultQueuePtr);

            for (int i=0; i<configPtrs.Length; ++i)
            {
                Librdkafka.ConfigResource_destroy(configPtrs[i]);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void CreatePartitions(
            IEnumerable<PartitionsSpecification> newPartitions,
            CreatePartitionsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options == null ? new CreatePartitionsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.CreatePartitions);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] newPartitionsPtrs = new IntPtr[newPartitions.Count()];
            try
            {
                int newPartitionsIdx = 0;
                foreach (var newPartitionsForTopic in newPartitions)
                {
                    var topic = newPartitionsForTopic.Topic;
                    var increaseTo = newPartitionsForTopic.IncreaseTo;
                    var assignments = newPartitionsForTopic.ReplicaAssignments;

                    if (newPartitionsForTopic.Topic == null)
                    {
                        throw new ArgumentException("Cannot add partitions to a null topic.");
                    }

                    IntPtr ptr = Librdkafka.NewPartitions_new(topic, (UIntPtr)increaseTo, errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                    if (ptr == IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(ErrorCode.Unknown, errorStringBuilder.ToString()));
                    }

                    if (assignments != null)
                    {
                        int assignmentsCount = 0;
                        foreach (var assignment in assignments)
                        {
                            errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
                            var brokerIds = assignments[assignmentsCount].ToArray();
                            var errorCode = Librdkafka.NewPartitions_set_replica_assignment(
                                ptr,
                                assignmentsCount,
                                brokerIds, (UIntPtr)brokerIds.Length,
                                errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                            if (errorCode != ErrorCode.NoError)
                            {
                                throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
                            }
                            assignmentsCount += 1;
                        }
                    }

                    newPartitionsPtrs[newPartitionsIdx] = ptr;
                    newPartitionsIdx += 1;
                }

                Librdkafka.CreatePartitions(handle, newPartitionsPtrs, (UIntPtr)newPartitionsPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var newPartitionPtr in newPartitionsPtrs)
                {
                    if (newPartitionPtr != IntPtr.Zero)
                    {
                        Librdkafka.NewPartitions_destroy(newPartitionPtr);
                    }
                }
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void DeleteRecords(
            IEnumerable<TopicPartitionOffset> topicPartitionOffsets,
            DeleteRecordsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DeleteRecordsOptions() : options;

            IntPtr deleteRecordsPtr = IntPtr.Zero;
            IntPtr optionsPtr = IntPtr.Zero;
            try
            {
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteRecords);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                if (topicPartitionOffsets.Where(tpo => tpo.Topic == null).Count() > 0)
                {
                    throw new ArgumentException("Cannot delete records because one or more topics were specified as null.");
                }

                IntPtr cOffsets = GetCTopicPartitionList(topicPartitionOffsets);
                if (cOffsets == IntPtr.Zero)
                {
                    throw new ArgumentNullException("Delete records offsets collection must not be null");
                }
                deleteRecordsPtr = Librdkafka.DeleteRecords_new(cOffsets);
                Librdkafka.topic_partition_list_destroy(cOffsets);

                IntPtr[] deleteRecordsPtrs = new IntPtr[1];
                deleteRecordsPtrs[0] = deleteRecordsPtr;
                Librdkafka.DeleteRecords(handle, deleteRecordsPtrs, (UIntPtr)1, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (deleteRecordsPtr != IntPtr.Zero)
                {
                    Librdkafka.DeleteRecords_destroy(deleteRecordsPtr);
                }

                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void DeleteGroups(IList<string> deleteGroups, DeleteGroupsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DeleteGroupsOptions() : options;

            IntPtr[] deleteGroupsPtrs = new IntPtr[deleteGroups.Count()];
            IntPtr optionsPtr = IntPtr.Zero;
            try
            {
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteGroups);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                for (int i = 0; i < deleteGroups.Count(); i++)
                {
                    deleteGroupsPtrs[i] = Librdkafka.DeleteGroup_new(deleteGroups[i]);
                }

                Librdkafka.DeleteGroups(handle, deleteGroupsPtrs, (UIntPtr)deleteGroupsPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach(var deleteGroupPtr in deleteGroupsPtrs)
                {
                    if(deleteGroupPtr != IntPtr.Zero)
                    {
                        Librdkafka.DeleteGroup_destroy(deleteGroupPtr);
                    }
                }

                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void DeleteConsumerGroupOffsets(String group, IEnumerable<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DeleteConsumerGroupOffsetsOptions() : options;
            var groupsToDeleteCnt = 1; // Offsets for only one group can be reset at a time

            IntPtr[] deleteGroupOffsetPtrs = new IntPtr[groupsToDeleteCnt];
            IntPtr optionsPtr = IntPtr.Zero;
            try
            {
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteConsumerGroupOffsets);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                if (partitions.Where(tp => tp.Topic == null).Count() > 0)
                {
                    throw new ArgumentException("Cannot delete offsets because one or more topics or partitions were specified as null.");
                }

                List<TopicPartitionOffset> topicPartitionOffsets = partitions.Select(a => new TopicPartitionOffset(a, Offset.Unset)).ToList();
                IntPtr cOffsets = GetCTopicPartitionList(topicPartitionOffsets);
                if (cOffsets == IntPtr.Zero)
                {
                    throw new ArgumentNullException("Delete offsets partitions collection must not be null");
                }

                deleteGroupOffsetPtrs[0] = Librdkafka.DeleteConsumerGroupOffsets_new(group, cOffsets); // Offsets for only one group can be reset at a time
                Librdkafka.DeleteConsumerGroupOffsets(handle, deleteGroupOffsetPtrs, (UIntPtr)deleteGroupOffsetPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var deleteGroupOffsetPtr in deleteGroupOffsetPtrs)
                {
                    if (deleteGroupOffsetPtr != IntPtr.Zero)
                    {
                        Librdkafka.DeleteConsumerGroupOffsets_destroy(deleteGroupOffsetPtr);
                    }
                }

                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void DeleteTopics(
            IEnumerable<string> deleteTopics,
            DeleteTopicsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DeleteTopicsOptions() : options;

            IntPtr[] deleteTopicsPtrs = new IntPtr[deleteTopics.Count()];
            IntPtr optionsPtr = IntPtr.Zero;
            try
            {
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteTopics);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                int idx = 0;
                foreach (var deleteTopic in deleteTopics)
                {
                    if (deleteTopic == null)
                    {
                        throw new ArgumentException("Cannot delete topics because one or more topics were specified as null.");
                    }

                    var deleteTopicPtr = Librdkafka.DeleteTopic_new(deleteTopic);
                    deleteTopicsPtrs[idx] = deleteTopicPtr;
                    idx += 1;
                }

                Librdkafka.DeleteTopics(handle, deleteTopicsPtrs, (UIntPtr)deleteTopicsPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var deleteTopicPtr in deleteTopicsPtrs)
                {
                    if (deleteTopicPtr != IntPtr.Zero)
                    {
                        Librdkafka.DeleteTopic_destroy(deleteTopicPtr);
                    }
                }

                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void CreateTopics(
            IEnumerable<TopicSpecification> newTopics,
            CreateTopicsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options == null ? new CreateTopicsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.CreateTopics);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] newTopicPtrs = new IntPtr[newTopics.Count()];
            try
            {
                int idx = 0;
                foreach (var newTopic in newTopics)
                {
                    if (newTopic.ReplicationFactor != -1 && newTopic.ReplicasAssignments != null)
                    {
                        throw new ArgumentException("ReplicationFactor must be -1 when ReplicasAssignments are specified.");
                    }

                    if (newTopic.Name == null)
                    {
                        throw new ArgumentException("Cannot create a topic with a name of null.");
                    }

                    IntPtr newTopicPtr = Librdkafka.NewTopic_new(
                        newTopic.Name,
                        (IntPtr)newTopic.NumPartitions,
                        (IntPtr)newTopic.ReplicationFactor,
                        errorStringBuilder,
                        (UIntPtr)errorStringBuilder.Capacity);
                    if (newTopicPtr == IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(ErrorCode.Unknown, errorStringBuilder.ToString()));
                    }

                    if (newTopic.ReplicasAssignments != null)
                    {
                        foreach (var replicAssignment in newTopic.ReplicasAssignments)
                        {
                            var partition = replicAssignment.Key;
                            var brokerIds = replicAssignment.Value.ToArray();
                            var errorCode = Librdkafka.NewTopic_set_replica_assignment(
                                                newTopicPtr,
                                                partition, brokerIds, (UIntPtr)brokerIds.Length,
                                                errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                            if (errorCode != ErrorCode.NoError)
                            {
                                throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
                            }
                        }
                    }

                    if (newTopic.Configs != null)
                    {
                        foreach (var config in newTopic.Configs)
                        {
                            Librdkafka.NewTopic_set_config(newTopicPtr, config.Key, config.Value);
                        }
                    }

                    newTopicPtrs[idx] = newTopicPtr;
                    idx += 1;
                }

                Librdkafka.CreateTopics(handle, newTopicPtrs, (UIntPtr)newTopicPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var newTopicPtr in newTopicPtrs)
                {
                    if (newTopicPtr != IntPtr.Zero)
                    {
                        Librdkafka.NewTopic_destroy(newTopicPtr);
                    }
                }
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        private static void Validate(AclBinding aclBinding)
        {
            if (aclBinding.Pattern == null) throw new ArgumentNullException("Pattern cannot be null");
            if (aclBinding.Entry == null) throw new ArgumentNullException("Entry cannot be null");
        }

        private static void Validate(AclBindingFilter aclBindingFilter)
        {
            if (aclBindingFilter.PatternFilter == null) throw new ArgumentNullException("PatternFilter cannot be null");
            if (aclBindingFilter.EntryFilter == null) throw new ArgumentNullException("EntryFilter cannot be null");
        }

        internal void CreateAcls(
            IEnumerable<AclBinding> aclBindings,
            CreateAclsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();
            if (aclBindings == null)
            {
                throw new ArgumentNullException("Expected non-null aclBindings argument");
            }
            if (aclBindings.Count() == 0)
            {
                throw new ArgumentException("Expected non-empty IEnumerable of AclBinding");
            }
            foreach (var aclBinding in aclBindings)
            {
                Validate(aclBinding);
            }

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options ?? new CreateAclsOptions();
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.CreateAcls);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] newAclsPtrs = new IntPtr[aclBindings.Count()];
            try
            {
                int idx = 0;
                foreach (var aclBinding in aclBindings)
                {
                    IntPtr newAclPtr = Librdkafka.AclBinding_new(
                        aclBinding.Pattern.Type,
                        aclBinding.Pattern.Name,
                        aclBinding.Pattern.ResourcePatternType,
                        aclBinding.Entry.Principal,
                        aclBinding.Entry.Host,
                        aclBinding.Entry.Operation,
                        aclBinding.Entry.PermissionType,
                        errorStringBuilder,
                        (UIntPtr)errorStringBuilder.Capacity
                    );
                    if (newAclPtr == IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(ErrorCode.Unknown, errorStringBuilder.ToString()));
                    }

                    newAclsPtrs[idx] = newAclPtr;
                    idx++;
                }
                Librdkafka.CreateAcls(handle, newAclsPtrs, (UIntPtr)newAclsPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var newAclPtr in newAclsPtrs)
                {
                    if (newAclPtr != IntPtr.Zero)
                    {
                        Librdkafka.AclBinding_destroy(newAclPtr);
                    }
                }
                Librdkafka.AdminOptions_destroy(optionsPtr);
            }
        }

        internal void DescribeAcls(
            AclBindingFilter aclBindingFilter,
            DescribeAclsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();
            if (aclBindingFilter == null)
            {
                throw new ArgumentNullException("Expected non-null AclBindingFilter");
            }
            Validate(aclBindingFilter);

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options ?? new DescribeAclsOptions();
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeAcls);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr aclBindingFilterPtr = IntPtr.Zero;
            try
            {
                aclBindingFilterPtr = Librdkafka.AclBindingFilter_new(
                    aclBindingFilter.PatternFilter.Type,
                    aclBindingFilter.PatternFilter.Name,
                    aclBindingFilter.PatternFilter.ResourcePatternType,
                    aclBindingFilter.EntryFilter.Principal,
                    aclBindingFilter.EntryFilter.Host,
                    aclBindingFilter.EntryFilter.Operation,
                    aclBindingFilter.EntryFilter.PermissionType,
                    errorStringBuilder,
                    (UIntPtr)errorStringBuilder.Capacity
                );
                if (aclBindingFilterPtr == IntPtr.Zero)
                {
                    throw new KafkaException(new Error(ErrorCode.Unknown, errorStringBuilder.ToString()));
                }
                Librdkafka.DescribeAcls(handle, aclBindingFilterPtr, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (aclBindingFilterPtr != IntPtr.Zero)
                {
                    Librdkafka.AclBinding_destroy(aclBindingFilterPtr);
                }
                Librdkafka.AdminOptions_destroy(optionsPtr);
            }
        }

        internal void DeleteAcls(
            IEnumerable<AclBindingFilter> aclBindingFilters,
            DeleteAclsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();
            if (aclBindingFilters.Count() == 0)
            {
                throw new ArgumentException("Expected non-empty IEnumerable of AclBindingFilter");
            }
            foreach (var aclBindingFilter in aclBindingFilters)
            {
                Validate(aclBindingFilter);
            }

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options ?? new DeleteAclsOptions();
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteAcls);
            setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] aclBindingFilterPtrs = new IntPtr[aclBindingFilters.Count()];
            try
            {
                int idx = 0;
                foreach (var aclBindingFilter in aclBindingFilters)
                {
                    IntPtr aclBindingFilterPtr = Librdkafka.AclBindingFilter_new(
                        aclBindingFilter.PatternFilter.Type,
                        aclBindingFilter.PatternFilter.Name,
                        aclBindingFilter.PatternFilter.ResourcePatternType,
                        aclBindingFilter.EntryFilter.Principal,
                        aclBindingFilter.EntryFilter.Host,
                        aclBindingFilter.EntryFilter.Operation,
                        aclBindingFilter.EntryFilter.PermissionType,
                        errorStringBuilder,
                        (UIntPtr)errorStringBuilder.Capacity
                    );
                    if (aclBindingFilterPtr == IntPtr.Zero)
                    {
                        throw new KafkaException(new Error(ErrorCode.Unknown, errorStringBuilder.ToString()));
                    }

                    aclBindingFilterPtrs[idx] = aclBindingFilterPtr;
                    idx++;
                }
                Librdkafka.DeleteAcls(handle, aclBindingFilterPtrs, (UIntPtr)aclBindingFilterPtrs.Length, optionsPtr, resultQueuePtr);
            }
            finally
            {
                foreach (var aclBindingFiltersPtr in aclBindingFilterPtrs)
                {
                    if (aclBindingFiltersPtr != IntPtr.Zero)
                    {
                        Librdkafka.AclBinding_destroy(aclBindingFiltersPtr);
                    }
                }
                Librdkafka.AdminOptions_destroy(optionsPtr);
            }
        }

        internal void AlterConsumerGroupOffsets(
            IEnumerable<ConsumerGroupTopicPartitionOffsets> groupsPartitions,
            AlterConsumerGroupOffsetsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

	        // For now, we only support one group at a time given as a single element of groupsPartitions.
	        // Code has been written so that only this if-guard needs to be removed when we add support for
	        // multiple ConsumerGroupTopicPartitionOffsets.
            if (groupsPartitions.Count() != 1) 
            {
                throw new ArgumentException("Can only alter offsets for one group at a time");
            }

            IntPtr optionsPtr = IntPtr.Zero;
            IntPtr[] groupsPartitionsPtrs = new IntPtr[groupsPartitions.Count()];

            try
            {
                // Set admin options if any
                options = options ?? new AlterConsumerGroupOffsetsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.AlterConsumerGroupOffsets);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Create the objects required by librdkafka to call the method.
                var idx = 0;
                foreach (var groupPartitions in groupsPartitions)
                {
                    if (groupPartitions == null)
                    {
                        throw new ArgumentException("Cannot alter consumer group offsets for null group");
                    }

                    // Create C list of topic partitions.
                    var list = SafeKafkaHandle.GetCTopicPartitionList(groupPartitions.TopicPartitionOffsets);

                    groupsPartitionsPtrs[idx] = Librdkafka.AlterConsumerGroupOffsets_new(groupPartitions.Group, list);
                    idx++;

                    if (list != IntPtr.Zero)
                    {
                        Librdkafka.topic_partition_list_destroy(list);
                    }
                }


                Librdkafka.AlterConsumerGroupOffsets(handle, groupsPartitionsPtrs, (UIntPtr)(uint)groupsPartitionsPtrs.Count(), optionsPtr, resultQueuePtr);
            }
            finally
            {
                // Clean up the options if created.
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }

                // Clean up the groupPartitionPtr objects if created.
                // Note that the function takes care of cleaning up the topic partition list inside the object.
                foreach (var groupPartitionPtr in groupsPartitionsPtrs)
                {
                    if (groupPartitionPtr != IntPtr.Zero)
                    {
                        Librdkafka.AlterConsumerGroupOffsets_destroy(groupPartitionPtr);
                    }
                }
            }

        }

        internal void ListConsumerGroupOffsets(
            IEnumerable<ConsumerGroupTopicPartitions> groupsPartitions,
            ListConsumerGroupOffsetsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

	        // For now, we only support one group at a time given as a single element of groupsPartitions.
	        // Code has been written so that only this if-guard needs to be removed when we add support for
	        // multiple ConsumerGroupTopicPartitions.
            if (groupsPartitions.Count() != 1) 
            {
                throw new ArgumentException("Can only list offsets for one group at a time");
            }

            IntPtr optionsPtr = IntPtr.Zero;
            IntPtr[] groupsPartitionPtrs = new IntPtr[groupsPartitions.Count()];

            try
            {
                // Set admin options if any
                options = options ?? new ListConsumerGroupOffsetsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.ListConsumerGroupOffsets);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_RequireStableOffsets(optionsPtr, options.RequireStableOffsets);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Create the objects required by librdkafka to call the method.
                var idx = 0;
                foreach (var groupPartitions in groupsPartitions)
                {
                    if (groupPartitions == null)
                    {
                        throw new ArgumentException("Cannot list consumer group offsets for null group");
                    }

                    // Create C list of topic partitions.
                    var list = SafeKafkaHandle.GetCTopicPartitionList(groupPartitions.TopicPartitions);

                    groupsPartitionPtrs[idx] = Librdkafka.ListConsumerGroupOffsets_new(groupPartitions.Group, list);
                    idx++;

                    if (list != IntPtr.Zero)
                    {
                        Librdkafka.topic_partition_list_destroy(list);
                    }
                }


                Librdkafka.ListConsumerGroupOffsets(handle, groupsPartitionPtrs, (UIntPtr)(uint)groupsPartitionPtrs.Count(), optionsPtr, resultQueuePtr);
            }
            finally
            {
                // Clean up the options if created.
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }

                // Clean up the groupsPartitionPtr objects if created.
                // Note that the function takes care of cleaning up the topic partition list inside the object.
                foreach (var groupsPartitionPtr in groupsPartitionPtrs)
                {
                    if (groupsPartitionPtr != IntPtr.Zero)
                    {
                        Librdkafka.ListConsumerGroupOffsets_destroy(groupsPartitionPtr);
                    }
                }
            }

        }

        internal void ListConsumerGroups(
            ListConsumerGroupsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            IntPtr optionsPtr = IntPtr.Zero;
            try
            {
                // Set Admin Options if any.
                options = options ?? new ListConsumerGroupsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.ListConsumerGroups);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                if (options.MatchStates != null)
                {
                    setOption_MatchConsumerGroupStates(optionsPtr, options.MatchStates.ToArray());
                }
                if (options.MatchTypes != null)
                {
                    setOption_MatchConsumerGroupTypes(optionsPtr, options.MatchTypes.ToArray());
                }
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Call ListConsumerGroups (async).
                Librdkafka.ListConsumerGroups(handle, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }

            }
        }


        internal void DescribeConsumerGroups(IEnumerable<string> groups, DescribeConsumerGroupsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            if (groups.Count() == 0) {
                throw new ArgumentException("at least one group should be provided to DescribeConsumerGroups");
            }

            var optionsPtr = IntPtr.Zero;
            try
            {
                // Set Admin Options if any.
                options = options ?? new DescribeConsumerGroupsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeConsumerGroups);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_IncludeAuthorizedOperations(optionsPtr, options.IncludeAuthorizedOperations);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Call DescribeConsumerGroups (async).
                Librdkafka.DescribeConsumerGroups(
                    handle, groups.ToArray(), (UIntPtr)(groups.Count()),
                    optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void DescribeUserScramCredentials(IEnumerable<string> users, DescribeUserScramCredentialsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            foreach (var user in users)
            {
                if (string.IsNullOrEmpty(user))
                {
                    throw new ArgumentException("Cannot have a null or empty user");
                }
            }
            
            var optionsPtr = IntPtr.Zero;
            try
            {
                // Set Admin Options if any.
                options = options ?? new DescribeUserScramCredentialsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeUserScramCredentials);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Call DescribeUserScramCredentials (async).
                Librdkafka.DescribeUserScramCredentials(
                    handle, users.ToArray(), (UIntPtr) users.Count(),
                    optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void AlterUserScramCredentials(IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var optionsPtr = IntPtr.Zero;
            IntPtr[] c_alterationsPtr = new IntPtr[alterations.Count()];
            var idx = 0;
            try
            {
                // Set Admin Options if any.
                options = options ?? new AlterUserScramCredentialsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.AlterUserScramCredentials);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                foreach (var alteration in alterations)
                {
                    if (alteration == null)
                    {
                        throw new ArgumentException("Cannot have a null alteration");
                    }

                    if (alteration.GetType() == typeof(UserScramCredentialDeletion))
                    {
                        UserScramCredentialDeletion deletion =
                            (UserScramCredentialDeletion)alteration;
                        c_alterationsPtr[idx] = Librdkafka.UserScramCredentialDeletion_new(deletion.User, deletion.Mechanism);
                        idx++;
                    }
                    else if (alteration.GetType() == typeof(UserScramCredentialUpsertion))
                    {
                        UserScramCredentialUpsertion upsertion =
                            (UserScramCredentialUpsertion)alteration;
                        byte[] salt = upsertion.Salt;
                        int saltSize = 0;
                        if (salt != null)
                            saltSize = salt.Length;
                        
                        c_alterationsPtr[idx] = Librdkafka.UserScramCredentialUpsertion_new(
                            upsertion.User,
                            upsertion.ScramCredentialInfo.Mechanism,
                            upsertion.ScramCredentialInfo.Iterations,
                            upsertion.Password,
                            (IntPtr) upsertion.Password.Length,
                            salt,
                            (IntPtr) saltSize
                        );
                        idx++;
                    }
                    else
                    {
                        throw new ArgumentException("Every alteration must be either a UserScramCredentialDeletion " + 
                            "or UserScramCredentialUpsertion");
                    }
                }
                Librdkafka.AlterUserScramCredentials(
                        handle, c_alterationsPtr, (UIntPtr)(alterations.Count()),
                        optionsPtr, resultQueuePtr);
            }
            finally
            {
                for(var i=0; i<idx; i++)
                {
                    Librdkafka.UserScramCredentialAlteration_destroy(c_alterationsPtr[i]);
                }
            }
        }

        internal void ListOffsets(IEnumerable<TopicPartitionOffsetSpec> topicPartitionOffsets, ListOffsetsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();
            var optionsPtr = IntPtr.Zero;
            var topic_partition_list = IntPtr.Zero;
            try
            {
                // set Admin Options if any
                options = options ?? new ListOffsetsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.ListOffsets);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_IsolationLevel(optionsPtr, options.IsolationLevel);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                topic_partition_list = Librdkafka.topic_partition_list_new((IntPtr)topicPartitionOffsets.Count());
                foreach(var topicPartitionOffset in topicPartitionOffsets)
                {
                    string topic = topicPartitionOffset.TopicPartition.Topic;
                    Partition partition = topicPartitionOffset.TopicPartition.Partition;
                    IntPtr topic_partition = Librdkafka.topic_partition_list_add(topic_partition_list, topic, partition);
                    Marshal.WriteInt64(
                        topic_partition,
                        (int) Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                        topicPartitionOffset.OffsetSpec.Value());
                }
                Librdkafka.ListOffsets(handle, topic_partition_list, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
                if (topic_partition_list != IntPtr.Zero)
                {
                    Librdkafka.topic_partition_list_destroy(topic_partition_list);
                }
            }
        }

        internal void ElectLeaders(ElectionType electionType, IEnumerable<TopicPartition> partitions, ElectLeadersOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();
            var optionsPtr = IntPtr.Zero;
            IntPtr topic_partition_list = IntPtr.Zero;
            IntPtr request = IntPtr.Zero;
            try
            {
                // Set Admin Options if any.
                options = new ElectLeadersOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.ElectLeaders);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_OperationTimeout(optionsPtr, options.OperationTimeout);
                setOption_completionSource(optionsPtr, completionSourcePtr);
                
                if(partitions != null)
                {
                       topic_partition_list = Librdkafka.topic_partition_list_new((IntPtr)partitions.Count());
                        foreach (var topicPartitions in partitions)
                        {
                            IntPtr topic_partition = Librdkafka.topic_partition_list_add(topic_partition_list, topicPartitions.Topic, topicPartitions.Partition);
                        }
                }
                request = Librdkafka.ElectLeadersRequest_New(electionType, topic_partition_list);

                Librdkafka.ElectLeaders(handle, request, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
                if (topic_partition_list != IntPtr.Zero)
                {
                    Librdkafka.topic_partition_list_destroy(topic_partition_list);
                }
                if(request != IntPtr.Zero)
                {
                    Librdkafka.ElectLeadersRequest_destroy(request);
                }
            }
        }

        internal void DescribeTopics(TopicCollection topicCollection, DescribeTopicsOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var optionsPtr = IntPtr.Zero;
            var topicCollectionPtr = IntPtr.Zero;
            try
            {
                topicCollectionPtr = Librdkafka.TopicCollection_of_topic_names(
                    topicCollection.Topics.ToArray(),
                    (UIntPtr)topicCollection.Topics.Count());
                
                // Set Admin Options if any.
                options = options ?? new DescribeTopicsOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeTopics);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_IncludeAuthorizedOperations(optionsPtr, options.IncludeAuthorizedOperations);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Call DescribeTopics (async).
                Librdkafka.DescribeTopics(
                    handle, topicCollectionPtr,
                    optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (topicCollectionPtr != IntPtr.Zero)
                {
                    Librdkafka.TopicCollection_destroy(topicCollectionPtr);
                }
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void DescribeCluster(DescribeClusterOptions options, IntPtr resultQueuePtr, IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var optionsPtr = IntPtr.Zero;
            try
            {
                // Set Admin Options if any.
                options = options ?? new DescribeClusterOptions();
                optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeCluster);
                setOption_RequestTimeout(optionsPtr, options.RequestTimeout);
                setOption_IncludeAuthorizedOperations(optionsPtr, options.IncludeAuthorizedOperations);
                setOption_completionSource(optionsPtr, completionSourcePtr);

                // Call DescribeCluster (async).
                Librdkafka.DescribeCluster(handle, optionsPtr, resultQueuePtr);
            }
            finally
            {
                if (optionsPtr != IntPtr.Zero)
                {
                    Librdkafka.AdminOptions_destroy(optionsPtr);
                }
            }
        }

        internal void OAuthBearerSetToken(string tokenValue, long lifetimeMs, string principalName, IDictionary<string, string> extensions)
        {
            if (tokenValue == null) throw new ArgumentNullException(nameof(tokenValue));

            var extensionsArray = extensions.ToStringArray();
            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
            var errorCode = Librdkafka.oauthbearer_set_token(handle,
                tokenValue, lifetimeMs, principalName,
                extensionsArray, (UIntPtr) (extensionsArray?.Length ?? 0),
                errorStringBuilder, (UIntPtr) errorStringBuilder.Capacity);

            if (errorCode != ErrorCode.NoError)
            {
                throw new KafkaException(CreatePossiblyFatalError(errorCode, errorStringBuilder.ToString()));
            }
        }

        internal void OAuthBearerSetTokenFailure(string errstr)
        {
            if (errstr == null) throw new ArgumentNullException(nameof(errstr));
            if (string.IsNullOrEmpty(errstr)) throw new ArgumentException($"Argument '{nameof(errstr)}' must be a non-empty string");

            var errorCode = Librdkafka.oauthbearer_set_token_failure(handle, errstr);

            if (errorCode != ErrorCode.NoError)
            {
                throw new KafkaException(errorCode);
            }
        }

        internal SafeTopicConfigHandle DuplicateDefaultTopicConfig()
            => Librdkafka.default_topic_conf_dup(this);

    }
}
