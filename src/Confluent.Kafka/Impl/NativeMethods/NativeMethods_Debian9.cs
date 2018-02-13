// Copyright 2016-2017 Confluent Inc.
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
// Refer to LICENSE for more information.

using System;
using System.Text;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka.Impl.NativeMethods
{
    /// <summary>
    ///     This class should be an exact replica of other NativeMethods classes, except
    ///     for the DllName const.
    /// </summary>
    /// <remarks>
    ///     This copy/pasting is required because DllName must be const. 
    ///     TODO: generate the NativeMethods classes at runtime (compile C# code) rather
    ///     than copy/paste.
    /// 
    ///     Alternatively, we could have used dlopen to load the native library, but to 
    ///     do that we need to know the absolute path of the native libraries because the
    ///     dlopen call does not know .NET runtime library storage conventions. Unfortunately 
    ///     these are relatively complex, so we prefer to go with the copy/paste solution
    ///     which is relatively simple.
    /// </remarks>
    internal class NativeMethods_Debian9
    {
        public const string DllName = "debian9-librdkafka";

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_version();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_version_str();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_get_debug_contexts();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_err2str(ErrorCode err);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_last_error();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_list_t * */ IntPtr
        rd_kafka_topic_partition_list_new(IntPtr size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_partition_list_destroy(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rkparlist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_t * */ IntPtr
        rd_kafka_topic_partition_list_add(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rktparlist,
                string topic, int partition);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_headers_t * */ IntPtr
        rd_kafka_headers_new(IntPtr size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_headers_destroy(
                /* rd_kafka_headers_t * */ IntPtr hdrs);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_header_add(
                /* rd_kafka_headers_t * */ IntPtr hdrs,
                /* const char * */ IntPtr name,
                /* ssize_t */ IntPtr name_size,
                /* const void * */ IntPtr value,
                /* ssize_t */ IntPtr value_size
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_header_get_all(
            /* const rd_kafka_headers_t * */ IntPtr hdrs,
            /* const size_t */ IntPtr idx,
            /* const char ** */ out IntPtr namep,
            /* const void ** */ out IntPtr valuep,
            /* size_t * */ out IntPtr sizep);
            
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* int64_t */ long rd_kafka_message_timestamp(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_timestamp_type_t * */ out IntPtr tstype);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_message_headers(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_headers_t * */ out IntPtr hdrs);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_message_destroy(
                /* rd_kafka_message_t * */ IntPtr rkmessage);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeConfigHandle rd_kafka_conf_new();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_destroy(IntPtr conf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_conf_dup(IntPtr conf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_conf_set(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_dr_msg_cb(
                IntPtr conf,
                LibRdKafka.DeliveryReportDelegate dr_msg_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf, LibRdKafka.RebalanceDelegate rebalance_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf, LibRdKafka.CommitDelegate commit_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_error_cb(
                IntPtr conf, LibRdKafka.ErrorDelegate error_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, LibRdKafka.LogDelegate log_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, LibRdKafka.StatsDelegate stats_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_default_topic_conf(
                IntPtr conf, IntPtr tconf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_conf_get(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                StringBuilder dest, ref UIntPtr dest_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_topic_conf_get(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                StringBuilder dest, ref UIntPtr dest_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char ** */ IntPtr rd_kafka_conf_dump(
                IntPtr conf, /* size_t * */ out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char ** */ IntPtr rd_kafka_topic_conf_dump(
                IntPtr conf, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_dump_free(/* const char ** */ IntPtr arr, UIntPtr cnt);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicConfigHandle rd_kafka_topic_conf_new();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_conf_t * */ IntPtr rd_kafka_topic_conf_dup(
                /* const rd_kafka_topic_conf_t * */ IntPtr conf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_destroy(IntPtr conf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_topic_conf_set(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                IntPtr topic_conf, LibRdKafka.PartitionerDelegate partitioner_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern bool rd_kafka_topic_partition_available(
                IntPtr rkt, int partition);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeKafkaHandle rd_kafka_new(
                RdKafkaType type, IntPtr conf,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_destroy(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_name(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* char * */ IntPtr rd_kafka_memberid(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicHandle rd_kafka_topic_new(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                /* rd_kafka_topic_conf_t * */ IntPtr conf);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_destroy(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_topic_name(IntPtr rkt);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_poll_set_consumer(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_poll(IntPtr rk, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_query_watermark_offsets(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition, out long low, out long high, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_get_watermark_offsets(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition, out long low, out long high);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_offsets_for_times(IntPtr rk,
            /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
            IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_mem_free(IntPtr rk, IntPtr ptr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_subscribe(IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr topics);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_unsubscribe(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_subscription(IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_message_t * */ IntPtr rd_kafka_consumer_poll(
                IntPtr rk, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_consumer_close(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_assign(IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_assignment(IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_offsets_store(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_commit(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                bool async);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_commit_queue(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                /* rd_kafka_queue_t * */ IntPtr rkqu,
                /* offset_commit_cb * */ LibRdKafka.CommitDelegate cb,
                /* void * */ IntPtr opaque);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_pause_partitions(
                IntPtr rk, IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_resume_partitions(
                IntPtr rk, IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_seek(
                IntPtr rkt, int partition, long offset, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_committed(
                IntPtr rk, IntPtr partitions, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_position(
                IntPtr rk, IntPtr partitions);

        // note: producev signature is rd_kafka_producev(rk, ...)
        // we are keeping things simple with one binding for now, but it 
        // will be worth benchmarking the overload with no timestamp, opaque,
        // partition, etc
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_producev(
            IntPtr rk,
            LibRdKafka.ProduceVarTag topicType, [MarshalAs(UnmanagedType.LPStr)] string topic,
            LibRdKafka.ProduceVarTag partitionType, int partition,
            LibRdKafka.ProduceVarTag vaType, IntPtr val, UIntPtr len,
            LibRdKafka.ProduceVarTag keyType, IntPtr key, UIntPtr keylen,
            LibRdKafka.ProduceVarTag msgflagsType, IntPtr msgflags,
            LibRdKafka.ProduceVarTag msg_opaqueType, IntPtr msg_opaque,
            LibRdKafka.ProduceVarTag timestampType, long timestamp,
            LibRdKafka.ProduceVarTag headersType, IntPtr headers,
            LibRdKafka.ProduceVarTag endType);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_flush(
            IntPtr rk,
            IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_metadata(
            IntPtr rk, bool all_topics,
            /* rd_kafka_topic_t * */ IntPtr only_rkt,
            /* const struct rd_kafka_metadata ** */ out IntPtr metadatap,
            IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_metadata_destroy(
                /* const struct rd_kafka_metadata * */ IntPtr metadata);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_list_groups(
                IntPtr rk, string group, out IntPtr grplistp,
                IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_group_list_destroy(
                IntPtr grplist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_brokers_add(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string brokerlist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_outq_len(IntPtr rk);

        //
        // Queues
        //
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_queue_new(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_queue_destroy(IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_queue_poll(IntPtr rkqu, IntPtr timeout_ms);

        //
        // Events
        //
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_event_destroy(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_event_type(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_event_error(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_error_string(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_topic_partition_list(IntPtr rkev);
    }
}