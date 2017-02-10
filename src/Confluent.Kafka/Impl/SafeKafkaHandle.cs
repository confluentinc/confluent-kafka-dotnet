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
using System.Text;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;
using System.Threading.Tasks;

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

        public SafeKafkaHandle() {}

        public static SafeKafkaHandle Create(RdKafkaType type, IntPtr config)
        {
            var errorStringBuilder = new StringBuilder(512);
            var skh = LibRdKafka.kafka_new(type, config, errorStringBuilder,
                    (UIntPtr) errorStringBuilder.Capacity);
            if (skh.IsInvalid)
            {
                LibRdKafka.conf_destroy(skh.DangerousGetHandle());
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            return skh;
        }

        protected override bool ReleaseHandle()
        {
            LibRdKafka.destroy(handle);
            return true;
        }

        private string name;
        internal string Name
        {
            get
            {
                if (name == null)
                {
                    name = Util.Marshal.PtrToStringUTF8(LibRdKafka.name(handle));
                }
                return name;
            }
        }

        internal long OutQueueLength
            => (long)LibRdKafka.outq_len(handle);

        internal long Flush(int millisecondsTimeout)
        {
            LibRdKafka.flush(handle, new IntPtr(millisecondsTimeout));
            return OutQueueLength;
        }

        internal long AddBrokers(string brokers)
            => (long)LibRdKafka.brokers_add(handle, brokers);

        internal long Poll(IntPtr millisecondsTimeout)
            => (long)LibRdKafka.poll(handle, millisecondsTimeout);

        internal SafeTopicHandle Topic(string topic, IntPtr config)
        {
            // Increase the refcount to this handle to keep it alive for
            // at least as long as the topic handle.
            // Will be decremented by the topic handle ReleaseHandle.
            bool success = false;
            DangerousAddRef(ref success);
            if (!success)
            {
                LibRdKafka.topic_conf_destroy(config);
                throw new Exception("Failed to create topic (DangerousAddRef failed)");
            }
            var topicHandle = LibRdKafka.topic_new(handle, topic, config);
            if (topicHandle.IsInvalid)
            {
                DangerousRelease();
                throw new KafkaException(LibRdKafka.last_error());
            }
            topicHandle.kafkaHandle = this;
            return topicHandle;
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
            IntPtr metaPtr;
            ErrorCode err = LibRdKafka.metadata(
                handle, allTopics,
                topic?.DangerousGetHandle() ?? IntPtr.Zero,
                /* const struct rd_kafka_metadata ** */ out metaPtr,
                (IntPtr) millisecondsTimeout);

            if (err == ErrorCode.NoError)
            {
                try {
                    var meta = (rd_kafka_metadata) Marshal.PtrToStructure<rd_kafka_metadata>(metaPtr);

                    var brokers = Enumerable.Range(0, meta.broker_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_broker>(
                                    meta.brokers + i * Marshal.SizeOf<rd_kafka_metadata_broker>()))
                        .Select(b => new BrokerMetadata(b.id, b.host, b.port))
                        .ToList();

                    var topics = Enumerable.Range(0, meta.topic_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_topic>(
                                    meta.topics + i * Marshal.SizeOf<rd_kafka_metadata_topic>()))
                        .Select(t => new TopicMetadata(
                                t.topic,
                                Enumerable.Range(0, t.partition_cnt)
                                    .Select(j => Marshal.PtrToStructure<rd_kafka_metadata_partition>(
                                                t.partitions + j * Marshal.SizeOf<rd_kafka_metadata_partition>()))
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
                    LibRdKafka.metadata_destroy(metaPtr);
                }
            }
            else
            {
                throw new KafkaException(err);
            }
        }

        internal ErrorCode PollSetConsumer()
            => LibRdKafka.poll_set_consumer(handle);

        internal WatermarkOffsets QueryWatermarkOffsets(string topic, int partition, int millisecondsTimeout)
        {
            long low;
            long high;

            ErrorCode err = LibRdKafka.query_watermark_offsets(handle, topic, partition, out low, out high, (IntPtr) millisecondsTimeout);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            return new WatermarkOffsets(low,  high);
        }

        internal WatermarkOffsets GetWatermarkOffsets(string topic, int partition)
        {
            long low;
            long high;

            ErrorCode err = LibRdKafka.get_watermark_offsets(handle, topic, partition, out low, out high);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            return new WatermarkOffsets(low, high);
        }

        internal void Subscribe(ICollection<string> topics)
        {
            IntPtr list = LibRdKafka.topic_partition_list_new((IntPtr) topics.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create topic partition list");
            }
            foreach (string topic in topics)
            {
                LibRdKafka.topic_partition_list_add(list, topic, RD_KAFKA_PARTITION_UA);
            }

            ErrorCode err = LibRdKafka.subscribe(handle, list);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }

        internal void Unsubscribe()
        {
            ErrorCode err = LibRdKafka.unsubscribe(handle);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }

        internal bool ConsumerPoll(out Message message, IntPtr millisecondsTimeout)
        {
            // TODO: There is a newer librdkafka interface for this now. Use that.
            IntPtr msgPtr = LibRdKafka.consumer_poll(handle, millisecondsTimeout);
            if (msgPtr == IntPtr.Zero)
            {
                message = null;
                return false;
            }

            var msg = Marshal.PtrToStructure<rd_kafka_message>(msgPtr);

            byte[] val = null;
            if (msg.val != IntPtr.Zero)
            {
                val = new byte[(int) msg.len];
                Marshal.Copy(msg.val, val, 0, (int) msg.len);
            }
            byte[] key = null;
            if (msg.key != IntPtr.Zero)
            {
                key = new byte[(int) msg.key_len];
                Marshal.Copy(msg.key, key, 0, (int) msg.key_len);
            }

            string topic = null;
            if (msg.rkt != IntPtr.Zero)
            {
                topic = Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt));
            }

            IntPtr timestampType;
            long timestamp = LibRdKafka.message_timestamp(msgPtr, out timestampType) / 1000;
            var dateTime = new DateTime(0);
            if ((TimestampType)timestampType != TimestampType.NotAvailable)
            {
                dateTime = Timestamp.UnixTimestampMsToDateTime(timestamp);
            }

            LibRdKafka.message_destroy(msgPtr);

            message = new Message(
                topic,
                msg.partition,
                msg.offset,
                key,
                val,
                new Timestamp(dateTime, (TimestampType)timestampType),
                msg.err
            );

            return true;
        }

        internal void ConsumerClose()
        {
            ErrorCode err = LibRdKafka.consumer_close(handle);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }

        internal List<TopicPartition> GetAssignment()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = LibRdKafka.assignment(handle, out listPtr);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }

            var ret = GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.TopicPartition).ToList();
            LibRdKafka.topic_partition_list_destroy(listPtr);
            return ret;
        }

        internal List<string> GetSubscription()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = LibRdKafka.subscription(handle, out listPtr);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
            var ret = GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.Topic).ToList();
            LibRdKafka.topic_partition_list_destroy(listPtr);
            return ret;
        }

        internal void Assign(ICollection<TopicPartitionOffset> partitions)
        {
            IntPtr list = IntPtr.Zero;
            if (partitions != null)
            {
                list = LibRdKafka.topic_partition_list_new((IntPtr) partitions.Count);
                if (list == IntPtr.Zero)
                {
                    throw new Exception("Failed to create topic partition list");
                }
                foreach (var partition in partitions)
                {
                    IntPtr ptr = LibRdKafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
                    Marshal.WriteInt64(
                        ptr,
                        (int) Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                        partition.Offset);
                }
            }

            ErrorCode err = LibRdKafka.assign(handle, list);
            if (list != IntPtr.Zero)
            {
                LibRdKafka.topic_partition_list_destroy(list);
            }
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
        }

        /// <summary>
        ///  Dummy commit callback that does nothing but prohibits
        ///  triggering the global offset_commit_cb.
        ///  Used by manual commits.
        /// </summary>
        static void dummyOffsetCommitCb (IntPtr rk, ErrorCode err, IntPtr offsets, IntPtr opaque)
        {
            return;
        }

        /// <summary>
        /// Manual sync commit, will block indefinately.
        /// </summary>
        /// <param name="offsets">Offsets to commit, or null for current assignment.</param>
        /// <returns>CommittedOffsets with global or per-partition errors.</returns>
        private CommittedOffsets commitSync (ICollection<TopicPartitionOffset> offsets)
        {
            // Create temporary queue so we can get the offset commit results
            // as an event instead of a callback.
            // We still need to specify a dummy callback (that does nothing)
            // to prevent the global offset_commit_cb to kick in.
            IntPtr cQueue = LibRdKafka.queue_new(handle);
            IntPtr cOffsets = GetCTopicPartitionList(offsets);
            ErrorCode err = LibRdKafka.commit_queue(handle, cOffsets, cQueue, dummyOffsetCommitCb, IntPtr.Zero);
            if (cOffsets != IntPtr.Zero)
                LibRdKafka.topic_partition_list_destroy(cOffsets);
            if (err != ErrorCode.NoError)
            {
                LibRdKafka.queue_destroy(cQueue);
                return new CommittedOffsets(new Error(err));
            }

            // Wait for commit to finish
            IntPtr rkev = LibRdKafka.queue_poll(cQueue, -1/*infinite*/);
            LibRdKafka.queue_destroy(cQueue);
            if (rkev == IntPtr.Zero)
            {
                // This shouldn't happen since timoeut is infinite
                return new CommittedOffsets(new Error(ErrorCode.Local_TimedOut));
            }

            CommittedOffsets committedOffsets =
                new CommittedOffsets(GetTopicPartitionOffsetErrorList(LibRdKafka.event_topic_partition_list(rkev)),
                new Error(LibRdKafka.event_error(rkev), LibRdKafka.event_error_string(rkev)));

            LibRdKafka.event_destroy(rkev);
            return committedOffsets;
        }

        internal async Task<CommittedOffsets> CommitAsync()
            => await Task.Run(() => commitSync(null));

        internal async Task<CommittedOffsets> CommitAsync(ICollection<TopicPartitionOffset> offsets)
            => await Task.Run(() => commitSync(offsets));

        /// <summary>
        ///     for each topic/partition returns the current committed offset
        ///     or a partition specific error. if no stored offset, Offset.Invalid.
        ///
        ///     throws KafakException if the above information cannot be retrieved.
        /// </summary>
        internal List<TopicPartitionOffsetError> Committed(ICollection<TopicPartition> partitions, IntPtr timeout_ms)
        {
            IntPtr list = LibRdKafka.topic_partition_list_new((IntPtr) partitions.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create committed partition list");
            }
            foreach (var partition in partitions)
            {
                LibRdKafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
            }
            ErrorCode err = LibRdKafka.committed(handle, list, timeout_ms);
            var result = GetTopicPartitionOffsetErrorList(list);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
            return result;
        }

        /// <summary>
        ///     for each topic/partition returns the current position (last consumed offset + 1)
        ///     or a partition specific error.
        ///
        ///     throws KafkaException if the above information cannot be retrieved.
        /// </summary>
        internal List<TopicPartitionOffsetError> Position(ICollection<TopicPartition> partitions)
        {
            IntPtr list = LibRdKafka.topic_partition_list_new((IntPtr) partitions.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create position list");
            }
            foreach (var partition in partitions)
            {
                LibRdKafka.topic_partition_list_add(list, partition.Topic, partition.Partition);
            }
            ErrorCode err = LibRdKafka.position(handle, list);
            var result = GetTopicPartitionOffsetErrorList(list);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NoError)
            {
                throw new KafkaException(err);
            }
            return result;
        }

        internal string MemberId
        {
            get
            {
                IntPtr strPtr = LibRdKafka.memberid(handle);
                if (strPtr == null)
                {
                    return null;
                }

                string memberId = Util.Marshal.PtrToStringUTF8(strPtr);
                LibRdKafka.mem_free(handle, strPtr);
                return memberId;
            }
        }

        internal static List<TopicPartitionOffsetError> GetTopicPartitionOffsetErrorList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<TopicPartitionOffsetError>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartitionOffsetError(
                        ktp.topic,
                        ktp.partition,
                        ktp.offset,
                        ktp.err
                    ))
                .ToList();
        }

        /// <summary>
        /// Creates and returns a C rd_kafka_topic_partition_list_t * populated by offsets.
        /// </summary>
        /// <returns>
        /// If offsets is null a null IntPtr will be returned, else a IntPtr
        /// which must destroyed with LibRdKafka.topic_partition_list_destroy()
        /// </returns>
        internal static IntPtr GetCTopicPartitionList(ICollection<TopicPartitionOffset> offsets)
        {
            if (offsets == null)
                return IntPtr.Zero;

            IntPtr list = LibRdKafka.topic_partition_list_new((IntPtr)offsets.Count);
            if (list == IntPtr.Zero)
                throw new OutOfMemoryException("Failed to create topic partition list");

            foreach (var p in offsets)
            {
                IntPtr ptr = LibRdKafka.topic_partition_list_add(list, p.Topic, p.Partition);
                Marshal.WriteInt64(ptr, (int)Marshal.OffsetOf<rd_kafka_topic_partition>("offset"), p.Offset);
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
            IntPtr grplistPtr;
            ErrorCode err = LibRdKafka.list_groups(handle, group, out grplistPtr, (IntPtr)millisecondsTimeout);
            if (err == ErrorCode.NoError)
            {
                var list = Marshal.PtrToStructure<rd_kafka_group_list>(grplistPtr);
                var groups = Enumerable.Range(0, list.group_cnt)
                    .Select(i => Marshal.PtrToStructure<rd_kafka_group_info>(
                        list.groups + i * Marshal.SizeOf<rd_kafka_group_info>()))
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
                                .Select(j => Marshal.PtrToStructure<rd_kafka_group_member_info>(
                                    gi.members + j * Marshal.SizeOf<rd_kafka_group_member_info>()))
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
                LibRdKafka.group_list_destroy(grplistPtr);
                return groups;
            }
            else
            {
                throw new KafkaException(err);
            }
        }
    }
}
