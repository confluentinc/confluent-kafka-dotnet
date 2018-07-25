// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
using System.Threading.Tasks;
using Confluent.Kafka;
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

        private bool flaggedAsClosed = false;

        /// <summary>
        ///     This functionality is awkward. It is intended that this method be called AFTER 
        ///     the handle has been disposed. The IsClosed property of the handle is set to true
        ///     immediately when Dispose is called, but calling rd_kafka_destroy() may have
        ///     side-effects that require use of the handle (which is valid during the dispose).
        ///     librdkafka should be changed such that close is not called during dispose.
        /// </summary>
        public void FlagAsClosed() => flaggedAsClosed = true;

        /// <summary>
        ///     Prevent AccessViolationException when handle has already been closed.
        ///     Should be called at start of every function using the handle,
        ///     except in ReleaseHandle. 
        /// </summary>
        protected override void ThrowIfHandleClosed()
        {
            if (flaggedAsClosed)
            {
                throw new ObjectDisposedException($"handle is closed", innerException: null);
            }
        }


        public SafeKafkaHandle() : base("kafka") { }

        public static SafeKafkaHandle Create(RdKafkaType type, IntPtr config)
        {
            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
            var skh = Librdkafka.kafka_new(type, config, errorStringBuilder,
                    (UIntPtr) errorStringBuilder.Capacity);
            if (skh.IsInvalid)
            {
                Librdkafka.conf_destroy(config);
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            return skh;
        }

        protected override bool ReleaseHandle()
        {
            Librdkafka.destroy(handle);
            return true;
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
            return (int)Librdkafka.brokers_add(handle, brokers);
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
        internal SafeTopicHandle Topic(string topic, IntPtr config)
        {
            ThrowIfHandleClosed();
            
            // Increase the refcount to this handle to keep it alive for
            // at least as long as the topic handle.
            // Will be decremented by the topic handle ReleaseHandle.
            bool success = false;
            DangerousAddRef(ref success);
            if (!success)
            {
                Librdkafka.topic_conf_destroy(config);
                throw new Exception("Failed to create topic (DangerousAddRef failed)");
            }
            var topicHandle = Librdkafka.topic_new(handle, topic, config);
            if (topicHandle.IsInvalid)
            {
                DangerousRelease();
                throw new KafkaException(Librdkafka.last_error());
            }
            topicHandle.kafkaHandle = this;
            return topicHandle;
        }

        private IntPtr marshalHeaders(IEnumerable<Header> headers)
        {
            var headersPtr = IntPtr.Zero;

            if (headers != null)
            {
                headersPtr = Librdkafka.headers_new((IntPtr) headers.Count());
                if (headersPtr == IntPtr.Zero)
                {
                    throw new Exception("Failed to create headers list.");
                }

                foreach (var header in headers)
                {
                    if (header.Key == null)
                    {
                        throw new ArgumentNullException("Message header keys must not be null.");
                    }
                    byte[] keyBytes = System.Text.UTF8Encoding.UTF8.GetBytes(header.Key);
                    GCHandle pinnedKey = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                    IntPtr keyPtr = pinnedKey.AddrOfPinnedObject();
                    IntPtr valuePtr = IntPtr.Zero;
                    GCHandle pinnedValue = default(GCHandle);
                    if (header.Value != null)
                    {
                        pinnedValue = GCHandle.Alloc(header.Value, GCHandleType.Pinned);
                        valuePtr = pinnedValue.AddrOfPinnedObject();
                    }
                    ErrorCode err = Librdkafka.headers_add(headersPtr, keyPtr, (IntPtr)keyBytes.Length, valuePtr, (IntPtr)header.Value.Length);
                    // copies of key and value have been made in headers_list_add - pinned values are no longer referenced.
                    pinnedKey.Free();
                    if (header.Value != null)
                    {
                        pinnedValue.Free();
                    }
                    if (err != ErrorCode.NoError)
                    {
                        throw new KafkaException(err);
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
            IEnumerable<Header> headers,
            IntPtr opaque,
            bool blockIfQueueFull)
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
                var errorCode = Librdkafka.producev(
                    handle,
                    topic,
                    partition,
                    (IntPtr)(MsgFlags.MSG_F_COPY | (blockIfQueueFull ? MsgFlags.MSG_F_BLOCK : 0)),
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
                throw new KafkaException(err);
            }
        }

        internal ErrorCode PollSetConsumer()
        {
            ThrowIfHandleClosed();
            return Librdkafka.poll_set_consumer(handle);
        }

        internal WatermarkOffsets QueryWatermarkOffsets(string topic, int partition, int millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.query_watermark_offsets(handle, topic, partition, out long low, out long high, (IntPtr)millisecondsTimeout);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            return new WatermarkOffsets(low,  high);
        }

        internal WatermarkOffsets GetWatermarkOffsets(string topic, int partition)
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.get_watermark_offsets(handle, topic, partition, out long low, out long high);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            return new WatermarkOffsets(low, high);
        }

        internal List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, int millisecondsTimeout)
        {
            var offsets = timestampsToSearch.Select(t => new TopicPartitionOffset(t.TopicPartition, t.Timestamp.UnixTimestampMs)).ToList();
            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            try
            {
                // The timestamps to query are represented as Offset property in offsets param on input, 
                // and Offset property will contain the offset on output
                var errorCode = Librdkafka.offsets_for_times(handle, cOffsets, (IntPtr) millisecondsTimeout);
                if (errorCode != ErrorCode.NoError)
                {
                    throw new KafkaException(errorCode);
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
                throw new KafkaException(err);
            }
        }

        internal void Unsubscribe()
        {
            ThrowIfHandleClosed();

            ErrorCode err = Librdkafka.unsubscribe(handle);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }

        internal IntPtr ConsumerPoll(
            bool enableTimestampMarshaling,
            bool enableHeaderMarshaling,
            IntPtr millisecondsTimeout)
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
                throw new KafkaException(err);
            }
        }

        internal List<TopicPartition> GetAssignment()
        {
            ThrowIfHandleClosed();

            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = Librdkafka.assignment(handle, out listPtr);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
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
                throw new KafkaException(err);
            }
            var ret = GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.Topic).ToList();
            Librdkafka.topic_partition_list_destroy(listPtr);
            return ret;
        }

        internal void Assign(IEnumerable<TopicPartitionOffset> partitions)
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
                    IntPtr ptr = Librdkafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
                    Marshal.WriteInt64(
                        ptr,
                        (int) Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                        partition.Offset);
                }
            }

            ErrorCode err = Librdkafka.assign(handle, list);
            if (list != IntPtr.Zero)
            {
                Librdkafka.topic_partition_list_destroy(list);
            }
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }


        internal List<TopicPartitionOffset> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
        {
            ThrowIfHandleClosed();

            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            ErrorCode err = Librdkafka.offsets_store(handle, cOffsets);
            var results = GetTopicPartitionOffsetErrorList(cOffsets);
            Librdkafka.topic_partition_list_destroy(cOffsets);

            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            if (results.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(results);
            }

            return results.Select(r => r.TopicPartitionOffset).ToList();
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


        internal List<TopicPartitionOffset> CommitSync(IEnumerable<TopicPartitionOffset> offsets)
        {
            ThrowIfHandleClosed();

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
                throw new KafkaException(err);
            }

            // Wait for commit to finish
            IntPtr rkev = Librdkafka.queue_poll(cQueue, -1/*infinite*/);
            Librdkafka.queue_destroy(cQueue);
            if (rkev == IntPtr.Zero)
            {
                // This shouldn't happen since timoeut is infinite
                throw new KafkaException(ErrorCode.Local_TimedOut);
            }

            var errorCode = Librdkafka.event_error(rkev);
            if (errorCode != ErrorCode.NoError)
            {
                var errorString = Librdkafka.event_error_string(rkev);
                Librdkafka.event_destroy(rkev);
                throw new KafkaException(new Error(errorCode, errorString));
            }
            
            var result = GetTopicPartitionOffsetErrorList(Librdkafka.event_topic_partition_list(rkev));
            Librdkafka.event_destroy(rkev);

            if (result.Where(tpoe => tpoe.Error.Code != ErrorCode.NoError).Count() > 0)
            {
                throw new TopicPartitionOffsetException(result);
            }

            return result.Select(r => r.TopicPartitionOffset).ToList();
        }


        internal void Seek(string topic, Partition partition, Offset offset, int millisecondsTimeout)
        {
            ThrowIfHandleClosed();

            SafeTopicHandle rtk = Topic(topic, IntPtr.Zero);
            var result = Librdkafka.seek(rtk.DangerousGetHandle(), partition, offset, (IntPtr)millisecondsTimeout);
            
            if (result != ErrorCode.NoError)
            {
                throw new KafkaException(result);
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
                throw new KafkaException(err);
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
                throw new KafkaException(err);
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
                throw new KafkaException(err);
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
                throw new KafkaException(err);
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
                return new List<TopicPartitionError>();
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
                return new List<TopicPartitionOffsetError>();
            }

            var list = Util.Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Util.Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Util.Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartitionOffsetError(
                        ktp.topic,
                        ktp.partition,
                        ktp.offset,
                        ktp.err
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
                return IntPtr.Zero;

            IntPtr list = Librdkafka.topic_partition_list_new((IntPtr)offsets.Count());
            if (list == IntPtr.Zero)
                throw new OutOfMemoryException("Failed to create topic partition list");

            foreach (var p in offsets)
            {
                IntPtr ptr = Librdkafka.topic_partition_list_add(list, p.Topic, p.Partition);
                Marshal.WriteInt64(ptr, (int)Util.Marshal.OffsetOf<rd_kafka_topic_partition>("offset"), p.Offset);
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
            =>  ListGroupsImpl(group, millisecondsTimeout).FirstOrDefault();

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
                throw new KafkaException(err);
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
                throw new KafkaException(new Error(errorCode, errorStringBuilder.ToString()));
            }
        }

        private void setOption_Timeout(IntPtr optionsPtr, TimeSpan? timeout)
        {
            if (timeout != null)
            {
                var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);
                var errorCode = Librdkafka.AdminOptions_set_request_timeout(optionsPtr, (IntPtr)(int)(timeout.Value.TotalMilliseconds), errorStringBuilder, (UIntPtr)errorStringBuilder.Capacity);
                if (errorCode != ErrorCode.NoError)
                {
                    throw new KafkaException(new Error(errorCode, errorStringBuilder.ToString()));
                }
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
            setOption_Timeout(optionsPtr, options.Timeout);
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
                        throw new KafkaException(errorCode);
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

        internal void DescribeConfigs(
            IEnumerable<ConfigResource> resources,
            DescribeConfigsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DescribeConfigsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DescribeConfigs);
            setOption_Timeout(optionsPtr, options.Timeout);
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
            IEnumerable<NewPartitions> newPartitions,
            CreatePartitionsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options == null ? new CreatePartitionsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.CreatePartitions);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_Timeout(optionsPtr, options.Timeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] newPartitionsPtrs = new IntPtr[newPartitions.Count()];
            int newPartitionsIdx = 0;
            foreach (var newPartitionsForTopic in newPartitions)
            {
                var topic = newPartitionsForTopic.Topic;
                var increaseTo = newPartitionsForTopic.IncreaseTo;
                var assignments = newPartitionsForTopic.Assignments;

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
                            throw new KafkaException(new Error(errorCode, errorStringBuilder.ToString()));
                        }
                        assignmentsCount += 1;
                    }
                }

                newPartitionsPtrs[newPartitionsIdx] = ptr;
                newPartitionsIdx += 1;
            }

            Librdkafka.CreatePartitions(handle, newPartitionsPtrs, (UIntPtr)newPartitionsPtrs.Length, optionsPtr, resultQueuePtr);

            foreach (var newPartitionPtr in newPartitionsPtrs)
            {
                Librdkafka.NewPartitions_destroy(newPartitionPtr);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void DeleteTopics(
            IEnumerable<string> deleteTopics,
            DeleteTopicsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            options = options == null ? new DeleteTopicsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.DeleteTopics);
            setOption_Timeout(optionsPtr, options.Timeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] deleteTopicsPtrs = new IntPtr[deleteTopics.Count()];
            int idx = 0;
            foreach (var deleteTopic in deleteTopics)
            {
                var deleteTopicPtr = Librdkafka.DeleteTopic_new(deleteTopic);
                deleteTopicsPtrs[idx] = deleteTopicPtr;
                idx += 1;
            }

            Librdkafka.DeleteTopics(handle, deleteTopicsPtrs, (UIntPtr)deleteTopicsPtrs.Length, optionsPtr, resultQueuePtr);

            foreach (var deleteTopicPtr in deleteTopicsPtrs)
            {
                Librdkafka.DeleteTopic_destroy(deleteTopicPtr);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

        internal void CreateTopics(
            IEnumerable<NewTopic> newTopics,
            CreateTopicsOptions options,
            IntPtr resultQueuePtr,
            IntPtr completionSourcePtr)
        {
            ThrowIfHandleClosed();

            var errorStringBuilder = new StringBuilder(Librdkafka.MaxErrorStringLength);

            options = options == null ? new CreateTopicsOptions() : options;
            IntPtr optionsPtr = Librdkafka.AdminOptions_new(handle, Librdkafka.AdminOp.CreateTopics);
            setOption_ValidatOnly(optionsPtr, options.ValidateOnly);
            setOption_Timeout(optionsPtr, options.Timeout);
            setOption_completionSource(optionsPtr, completionSourcePtr);

            IntPtr[] newTopicPtrs = new IntPtr[newTopics.Count()];
            int idx = 0;
            foreach (var newTopic in newTopics)
            {
                if (newTopic.ReplicationFactor != -1 && newTopic.ReplicasAssignments != null)
                {
                    throw new ArgumentException("ReplicationFactor must be -1 when ReplicasAssignments are specified.");
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
                            throw new KafkaException(new Error(errorCode, errorStringBuilder.ToString()));
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

            foreach (var newTopicPtr in newTopicPtrs)
            {
                Librdkafka.NewTopic_destroy(newTopicPtr);
            }

            Librdkafka.AdminOptions_destroy(optionsPtr);
        }

    }
}
