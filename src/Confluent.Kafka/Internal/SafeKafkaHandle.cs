using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal
{
    enum RdKafkaType
    {
        Producer,
        Consumer
    }

    [StructLayout(LayoutKind.Sequential)]
    struct rd_kafka_message
    {
        internal ErrorCode err; /* Non-zero for error signaling. */
        internal /* rd_kafka_topic_t * */ IntPtr rkt; /* Topic */
        internal int partition;                 /* Partition */
        internal /* void   * */ IntPtr payload; /* err==0: Message payload
                                        * err!=0: Error string */
        internal UIntPtr  len;                  /* err==0: Message payload length
                                        * err!=0: Error string length */
        internal /* void   * */ IntPtr key;     /* err==0: Optional message key */
        internal UIntPtr  key_len;              /* err==0: Optional message key length */
        internal long offset;                  /* Consume:
                                        *   Message offset (or offset for error
                                        *   if err!=0 if applicable).
                                        * dr_msg_cb:
                                        *   Message offset assigned by broker.
                                        *   If produce.offset.report is set then
                                        *   each message will have this field set,
                                        *   otherwise only the last message in
                                        *   each produced internal batch will
                                        *   have this field set, otherwise 0. */
        internal /* void  * */ IntPtr _private; /* Consume:
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
        /* void * */ IntPtr metadata;
        UIntPtr metadata_size;
        /* void * */ IntPtr opaque;
        ErrorCode err; /* Error code, depending on use. */
        /* void * */ IntPtr _private; /* INTERNAL USE ONLY,
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
        const int RD_KAFKA_PARTITION_UA = -1;

        private SafeKafkaHandle() {}

        internal static SafeKafkaHandle Create(RdKafkaType type, IntPtr config)
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

        internal string GetName() => Marshal.PtrToStringAnsi(LibRdKafka.name(handle));

        internal long GetOutQueueLength() => (long)LibRdKafka.outq_len(handle);

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

        /*
         *  allTopics  - if true: request info about all topics in cluster,
         *               else: only request info about locally known topics.
         *  onlyTopic  - only request info about this topic
         *  timeout    - maximum response time before failing.
         */
        internal Metadata Metadata(bool allTopics,
                SafeTopicHandle onlyTopic,
                bool includeInternal,
                TimeSpan timeout)
        {
            if (timeout == default(TimeSpan))
            {
                timeout = TimeSpan.FromSeconds(10);
            }

            IntPtr metaPtr;
            ErrorCode err = LibRdKafka.metadata(
                handle, allTopics,
                onlyTopic?.DangerousGetHandle() ?? IntPtr.Zero,
                /* const struct rd_kafka_metadata ** */ out metaPtr,
                (IntPtr) timeout.TotalMilliseconds);

            if (err == ErrorCode.NO_ERROR)
            {
                try {
                    var meta = (rd_kafka_metadata) Marshal.PtrToStructure<rd_kafka_metadata>(metaPtr);

                    var brokers = Enumerable.Range(0, meta.broker_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_broker>(
                                    meta.brokers + i * Marshal.SizeOf<rd_kafka_metadata_broker>()))
                        .Select(b => new BrokerMetadata() { BrokerId = b.id, Host = b.host, Port = b.port })
                        .ToList();

                    // TODO: filter our topics starting with __, as those are internal. Maybe add a flag to not ignore them.
                    var topics = Enumerable.Range(0, meta.topic_cnt)
                        .Select(i => Marshal.PtrToStructure<rd_kafka_metadata_topic>(
                                    meta.topics + i * Marshal.SizeOf<rd_kafka_metadata_topic>()))
                        .Where(t => includeInternal || !t.topic.StartsWith("__"))
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

        internal Offsets QueryWatermarkOffsets(string topic, int partition, TimeSpan timeout)
        {
            long low;
            long high;

            ErrorCode err = LibRdKafka.query_watermark_offsets(handle, topic, partition, out low, out high,
                    timeout == default(TimeSpan) ?  new IntPtr(-1) : (IntPtr) timeout.TotalMilliseconds);
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

        internal MessageAndError? ConsumerPoll(IntPtr timeoutMs)
        {
            IntPtr msgPtr = LibRdKafka.consumer_poll(handle, timeoutMs);
            if (msgPtr == IntPtr.Zero)
            {
                return null;
            }
            var msg = Marshal.PtrToStructure<rd_kafka_message>(msgPtr);
            byte[] payload = null;
            byte[] key = null;
            if (msg.payload != IntPtr.Zero)
            {
                payload = new byte[(int) msg.len];
                Marshal.Copy(msg.payload, payload, 0, (int) msg.len);
            }
            if (msg.key != IntPtr.Zero)
            {
                key = new byte[(int) msg.key_len];
                Marshal.Copy(msg.key, key, 0, (int) msg.key_len);
            }
            string topic = null;
            if (msg.rkt != IntPtr.Zero)
            {
                topic = Marshal.PtrToStringAnsi(LibRdKafka.topic_name(msg.rkt));
            }
            LibRdKafka.message_destroy(msgPtr);

            var message = new Message()
            {
                Topic = topic,
                Partition = msg.partition,
                Offset = msg.offset,
                Payload = payload,
                Key = key
            };

            return new MessageAndError()
            {
                Message = message,
                Error = msg.err
            };
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
            return GetTopicPartitionList(listPtr);
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
            return GetTopicList(listPtr);
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
                    Marshal.WriteInt64(ptr,
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
                Marshal.WriteInt64(ptr,
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

        internal List<TopicPartitionOffset> Committed(ICollection<TopicPartition> partitions, IntPtr timeout_ms)
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
            var result = GetTopicPartitionOffsetList(list);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to fetch committed offsets");
            }
            return result;
        }

        internal List<TopicPartitionOffset> Position(ICollection<TopicPartition> partitions)
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
            var result = GetTopicPartitionOffsetList(list);
            LibRdKafka.topic_partition_list_destroy(list);
            if (err != ErrorCode.NO_ERROR)
            {
                throw RdKafkaException.FromErr(err, "Failed to fetch position");
            }
            return result;
        }

        internal string MemberId()
        {
            IntPtr strPtr = LibRdKafka.memberid(handle);
            if (strPtr == null)
            {
                return null;
            }

            string memberId = Marshal.PtrToStringAnsi(strPtr);
            LibRdKafka.mem_free(handle, strPtr);
            return memberId;
        }

        internal void SetLogLevel(int level)
        {
            LibRdKafka.set_log_level(handle, (IntPtr) level);
        }

        internal static List<string> GetTopicList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<string>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => ktp.topic)
                .ToList();
        }

        internal static List<TopicPartition> GetTopicPartitionList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<TopicPartition>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartition()
                        {
                            Topic = ktp.topic,
                            Partition = ktp.partition,
                        })
                .ToList();
        }

        internal static List<TopicPartitionOffset> GetTopicPartitionOffsetList(IntPtr listPtr)
        {
            if (listPtr == IntPtr.Zero)
            {
                return new List<TopicPartitionOffset>();
            }

            var list = Marshal.PtrToStructure<rd_kafka_topic_partition_list>(listPtr);
            return Enumerable.Range(0, list.cnt)
                .Select(i => Marshal.PtrToStructure<rd_kafka_topic_partition>(
                    list.elems + i * Marshal.SizeOf<rd_kafka_topic_partition>()))
                .Select(ktp => new TopicPartitionOffset()
                        {
                            Topic = ktp.topic,
                            Partition = ktp.partition,
                            Offset = ktp.offset
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

        internal List<GroupInfo> ListGroups(string group, IntPtr timeoutMs)
        {
            IntPtr grplistPtr;
            ErrorCode err = LibRdKafka.list_groups(handle, group, out grplistPtr, timeoutMs);
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
                                                MemberMetadata = CopyBytes(mi.member_metadata,
                                                        mi.member_metadata_size),
                                                MemberAssignment = CopyBytes(mi.member_assignment,
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
