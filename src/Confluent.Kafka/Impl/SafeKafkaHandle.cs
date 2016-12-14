using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
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

        public SafeKafkaHandle() {}

        public static SafeKafkaHandle Create(RdKafkaType type, IntPtr config)
        {
            var errorStringBuilder = new StringBuilder(512);
            var skh = LibRdKafka.kafka_new(type, config, errorStringBuilder,
                    (UIntPtr) errorStringBuilder.Capacity);
            if (skh.IsInvalid)
            {
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
        {
            get
            {
                return (long)LibRdKafka.outq_len(handle);
            }
        }

        internal void Flush(TimeSpan timeout)
        {
            LibRdKafka.flush(handle, new IntPtr((int)timeout.TotalMilliseconds));
        }

        internal long AddBrokers(string brokers) => (long)LibRdKafka.brokers_add(handle, brokers);

        internal long Poll(IntPtr timeoutMs) => (long)LibRdKafka.poll(handle, timeoutMs);

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
                throw RdKafkaException.FromErr(LibRdKafka.last_error(), "Failed to create topic");
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
        internal Metadata GetMetadata(bool allTopics, SafeTopicHandle topic, TimeSpan? timeout)
        {
            int timeoutMs = (int)(timeout?.TotalMilliseconds ?? -1);

            IntPtr metaPtr;
            ErrorCode err = LibRdKafka.metadata(
                handle, allTopics,
                topic?.DangerousGetHandle() ?? IntPtr.Zero,
                /* const struct rd_kafka_metadata ** */ out metaPtr,
                (IntPtr) timeoutMs);

            if (err == ErrorCode.NO_ERROR)
            {
                try {
                    var meta = (rd_kafka_metadata) Marshal.PtrToStructure<rd_kafka_metadata>(metaPtr);

                    var brokers = Enumerable.Range(0, meta.broker_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_broker>(
                                    meta.brokers + i * Marshal.SizeOf<rd_kafka_metadata_broker>()))
                        .Select(b => new BrokerMetadata() { BrokerId = b.id, Host = b.host, Port = b.port })
                        .ToList();

                    var topics = Enumerable.Range(0, meta.topic_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_topic>(
                                    meta.topics + i * Marshal.SizeOf<rd_kafka_metadata_topic>()))
                        .Select(t => new TopicMetadata()
                            {
                                Topic = t.topic,
                                Error = t.err,
                                Partitions =
                                    Enumerable.Range(0, t.partition_cnt)
                                    .Select(j => Marshal.PtrToStructure<rd_kafka_metadata_partition>(
                                                t.partitions + j * Marshal.SizeOf<rd_kafka_metadata_partition>()))
                                    .Select(p => new PartitionMetadata()
                                        {
                                            PartitionId = p.id,
                                            Error = p.err,
                                            Leader = p.leader,
                                            Replicas = MarshalCopy(p.replicas, p.replica_cnt),
                                            InSyncReplicas = MarshalCopy(p.isrs, p.isr_cnt)
                                        })
                                    .ToList()
                            })
                        .ToList();

                    return new Metadata()
                    {
                        Brokers = brokers,
                        Topics = topics,
                        OriginatingBrokerId = meta.orig_broker_id,
                        OriginatingBrokerName = meta.orig_broker_name
                    };
                }
                finally
                {
                    LibRdKafka.metadata_destroy(metaPtr);
                }
            }
            else
            {
                throw RdKafkaException.FromErr(err, "Could not retrieve metadata");
            }
        }

        internal Offsets QueryWatermarkOffsets(string topic, int partition, TimeSpan? timeout)
        {
            int timeoutMs = (int)(timeout?.TotalMilliseconds ?? -1);

            long low;
            long high;

            ErrorCode err = LibRdKafka.query_watermark_offsets(handle, topic, partition, out low, out high, (IntPtr) timeoutMs);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to query watermark offsets");
            }

            return new Offsets { Low = low, High = high };
        }

        internal Offsets GetWatermarkOffsets(string topic, int partition)
        {
            long low;
            long high;

            ErrorCode err = LibRdKafka.get_watermark_offsets(handle, topic, partition, out low, out high);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to get watermark offsets");
            }

            return new Offsets { Low = low, High = high };
        }

        // Consumer API
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
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to subscribe to topics");
            }
        }

        internal void Unsubscribe()
        {
            ErrorCode err = LibRdKafka.unsubscribe(handle);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to unsubscribe");
            }
        }

        internal MessageInfo? ConsumerPoll(IntPtr timeoutMs)
        {
            // TODO: This actually triggers rebalance callbacks etc.
            // There is an alternative interface now, which returns these separately as events.
            IntPtr msgPtr = LibRdKafka.consumer_poll(handle, timeoutMs);
            if (msgPtr == IntPtr.Zero)
            {
                return null;
            }

            var msg = Marshal.PtrToStructure<rd_kafka_message>(msgPtr);

            var result = new MessageInfo();

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

            result.Key = key;
            result.Value = val;

            result.Topic = null;
            if (msg.rkt != IntPtr.Zero)
            {
                result.Topic = Util.Marshal.PtrToStringUTF8(LibRdKafka.topic_name(msg.rkt));
            }

            IntPtr timestampType;
            long timestamp = LibRdKafka.message_timestamp(msgPtr, out timestampType) / 1000;
            var dateTime = new DateTime(0);
            if ((TimestampType)timestampType != TimestampType.NotAvailable)
            {
                // TODO: is timestamp guarenteed to be in valid range if type == NotAvailable? if so, remove this conditional.
                dateTime = Timestamp.UnixTimestampMsToDateTime(timestamp);
            }
            result.Timestamp = new Timestamp(dateTime, (TimestampType)timestampType);

            result.Partition = msg.partition;
            result.Offset = msg.offset;
            result.Error = new Error(
                msg.err,
                Util.Marshal.PtrToStringUTF8(LibRdKafka.err2str(msg.err))
            );

            LibRdKafka.message_destroy(msgPtr);

            return result;
        }

        internal void ConsumerClose()
        {
            ErrorCode err = LibRdKafka.consumer_close(handle);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to close consumer");
            }
        }

        internal List<TopicPartition> GetAssignment()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = LibRdKafka.assignment(handle, out listPtr);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to get assignment");
            }
            // TODO: need to free anything here?
            return GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.TopicPartition).ToList();
        }

        internal List<string> GetSubscription()
        {
            IntPtr listPtr = IntPtr.Zero;
            ErrorCode err = LibRdKafka.subscription(handle, out listPtr);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to get subscription");
            }
            // TODO: need to free anything here?
            return GetTopicPartitionOffsetErrorList(listPtr).Select(a => a.Topic).ToList();
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
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to assign partitions");
            }
        }

        internal void Commit()
        {
            ErrorCode err = LibRdKafka.commit(handle, IntPtr.Zero, false);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to commit offsets");
            }
        }

        internal void Commit(ICollection<TopicPartitionOffset> offsets)
        {
            IntPtr list = LibRdKafka.topic_partition_list_new((IntPtr) offsets.Count);
            if (list == IntPtr.Zero)
            {
                throw new Exception("Failed to create offset commit list");
            }
            foreach (var offset in offsets)
            {
                IntPtr ptr = LibRdKafka.topic_partition_list_add(list, offset.Topic, offset.Partition);
                Marshal.WriteInt64(
                    ptr,
                    (int) Marshal.OffsetOf<rd_kafka_topic_partition>("offset"),
                    offset.Offset);
            }
            ErrorCode err = LibRdKafka.commit(handle, list, false);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to commit offsets");
            }
        }

        /// <summary>
        ///     for each topic/partition returns the current committed offset
        ///     or a partition specific error. if no stored offset, Offset.Invalid.
        ///
        ///     throws RdKafakException if the above information cannot be got.
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
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to fetch committed offsets");
            }
            return result;
        }

        /// <summary>
        ///     for each topic/partition returns the current position (last commited offset + 1)
        ///     or a partition specific error.
        ///
        ///     throws RdKafakException if the above information cannot be got.
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
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to fetch position");
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
                .Select(ktp => new TopicPartitionOffsetError()
                    {
                        Topic = ktp.topic,
                        Partition = ktp.partition,
                        Offset = ktp.offset,
                        Error = new Error(
                            ktp.err,
                            Util.Marshal.PtrToStringUTF8(LibRdKafka.err2str(ktp.err))
                        )
                    })
                .ToList();
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

        internal GroupInfo ListGroup(string group, TimeSpan? timeout)
        {
            return ListGroupsImpl(group, timeout).Single();
        }

        internal List<GroupInfo> ListGroups(TimeSpan? timeout)
        {
            return ListGroupsImpl(null, timeout);
        }

        private List<GroupInfo> ListGroupsImpl(string group, TimeSpan? timeout)
        {
            int timeoutMs = (int)(timeout?.TotalMilliseconds ?? -1);

            IntPtr grplistPtr;
            ErrorCode err = LibRdKafka.list_groups(handle, group, out grplistPtr, (IntPtr)timeoutMs);
            if (err == ErrorCode.NO_ERROR)
            {
                var list = Marshal.PtrToStructure<rd_kafka_group_list>(grplistPtr);
                var groups = Enumerable.Range(0, list.group_cnt)
                    .Select(i => Marshal.PtrToStructure<rd_kafka_group_info>(
                        list.groups + i * Marshal.SizeOf<rd_kafka_group_info>()))
                    .Select(gi => new GroupInfo()
                        {
                            Broker = new BrokerMetadata()
                            {
                                BrokerId = gi.broker.id,
                                Host = gi.broker.host,
                                Port = gi.broker.port
                            },
                            Group = gi.group,
                            Error = gi.err,
                            State = gi.state,
                            ProtocolType = gi.protocol_type,
                            Protocol = gi.protocol,
                            Members = Enumerable.Range(0, gi.member_cnt)
                                .Select(j => Marshal.PtrToStructure<rd_kafka_group_member_info>(
                                    gi.members + j * Marshal.SizeOf<rd_kafka_group_member_info>()))
                                .Select(mi => new GroupMemberInfo()
                                    {
                                        MemberId = mi.member_id,
                                        ClientId = mi.client_id,
                                        ClientHost = mi.client_host,
                                        MemberMetadata = CopyBytes(
                                            mi.member_metadata,
                                            mi.member_metadata_size),
                                        MemberAssignment = CopyBytes(
                                            mi.member_assignment,
                                            mi.member_assignment_size)
                                    })
                                .ToList()
                        })
                    .ToList();
                LibRdKafka.group_list_destroy(grplistPtr);
                return groups;
            }
            else
            {
                throw RdKafkaException.FromErr(err, "Failed to fetch group list");
            }
        }
    }
}
