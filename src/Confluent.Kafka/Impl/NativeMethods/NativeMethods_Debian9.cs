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
using Confluent.Kafka.Admin;


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
        internal static extern ErrorCode rd_kafka_fatal_error(
                IntPtr rk,
                StringBuilder errstr,
                UIntPtr errstr_size);
                
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
        internal static extern ErrorCode rd_kafka_header_get_all(
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
        internal static extern PersistenceStatus rd_kafka_message_status(
                /* rd_kafka_message_t * */ IntPtr rkmessage);

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
                Librdkafka.DeliveryReportDelegate dr_msg_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf, Librdkafka.RebalanceDelegate rebalance_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf, Librdkafka.CommitDelegate commit_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_error_cb(
                IntPtr conf, Librdkafka.ErrorDelegate error_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, Librdkafka.LogDelegate log_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_oauthbearer_token_refresh_cb(IntPtr conf, Librdkafka.OAuthBearerTokenRefreshDelegate oauthbearer_token_refresh_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_oauthbearer_set_token(
            IntPtr rk,
            [MarshalAs(UnmanagedType.LPStr)] string token_value,
            long md_lifetime_ms,
            [MarshalAs(UnmanagedType.LPStr)] string md_principal_name,
            [MarshalAs(UnmanagedType.LPArray)] string[] extensions, UIntPtr extension_size,
            StringBuilder errstr, UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_oauthbearer_set_token_failure(
            IntPtr rk,
            [MarshalAs(UnmanagedType.LPStr)] string errstr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, Librdkafka.StatsDelegate stats_cb);

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
                IntPtr topic_conf, Librdkafka.PartitionerDelegate partitioner_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern bool rd_kafka_topic_partition_available(
                IntPtr rkt, int partition);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_init_transactions(
                IntPtr rk, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_begin_transaction(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_commit_transaction(
                IntPtr rk, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_abort_transaction(
                IntPtr rk, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_send_offsets_to_transaction(
                IntPtr rk, IntPtr offsets, IntPtr consumer_group_metadata,
                IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_consumer_group_metadata_destroy(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata_write(
                /* rd_kafka_consumer_group_metadata_t * */IntPtr cgmd,
                /* const void ** */ out IntPtr valuep,
                /* size_t * */ out IntPtr sizep);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata_read(
                /* rd_kafka_consumer_group_metadata_t ** */ out IntPtr cgmdp,
                byte[] buffer, IntPtr size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeKafkaHandle rd_kafka_new(
                RdKafkaType type, IntPtr conf,
                StringBuilder errstr,
                UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_destroy(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_destroy_flags(IntPtr rk, IntPtr flags);

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
                /* offset_commit_cb * */ Librdkafka.CommitDelegate cb,
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
            Librdkafka.ProduceVarTag topicType, [MarshalAs(UnmanagedType.LPStr)] string topic,
            Librdkafka.ProduceVarTag partitionType, int partition,
            Librdkafka.ProduceVarTag vaType, IntPtr val, UIntPtr len,
            Librdkafka.ProduceVarTag keyType, IntPtr key, UIntPtr keylen,
            Librdkafka.ProduceVarTag msgflagsType, IntPtr msgflags,
            Librdkafka.ProduceVarTag msg_opaqueType, IntPtr msg_opaque,
            Librdkafka.ProduceVarTag timestampType, long timestamp,
            Librdkafka.ProduceVarTag headersType, IntPtr headers,
            Librdkafka.ProduceVarTag endType);

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
        // Admin API
        //

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_new(IntPtr rk, Librdkafka.AdminOp op);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AdminOptions_destroy(IntPtr options);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_request_timeout(
                        IntPtr options,
                        IntPtr timeout_ms,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_operation_timeout(
                        IntPtr options,
                        IntPtr timeout_ms,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_validate_only(
                        IntPtr options,
                        IntPtr true_or_false,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_incremental(
                        IntPtr options,
                        IntPtr true_or_false,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_broker(
                        IntPtr options,
                        int broker_id,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AdminOptions_set_opaque(
                        IntPtr options,
                        IntPtr opaque);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_NewTopic_new(
                        [MarshalAs(UnmanagedType.LPStr)] string topic,
                        IntPtr num_partitions,
                        IntPtr replication_factor,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_NewTopic_destroy(
                        IntPtr new_topic);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewTopic_set_replica_assignment(
                        IntPtr new_topic,
                        int partition,
                        int[] broker_ids,
                        UIntPtr broker_id_cnt,
                        StringBuilder errstr,
                        UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewTopic_set_config(
                        IntPtr new_topic,
                        [MarshalAs(UnmanagedType.LPStr)] string name,
                        [MarshalAs(UnmanagedType.LPStr)] string value);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreateTopics(
                        /* rd_kafka_t * */ IntPtr rk,
                        /* rd_kafka_NewTopic_t ** */ IntPtr[] new_topics,
                        UIntPtr new_topic_cnt,
                        /* rd_kafka_AdminOptions_t * */ IntPtr options,
                        /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_CreateTopics_result_topics(
                /* rd_kafka_CreateTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
        );


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteTopic_t * */ IntPtr rd_kafka_DeleteTopic_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteTopic_destroy(
                /* rd_kafka_DeleteTopic_t * */ IntPtr del_topic);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteTopics(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteTopic_t ** */ IntPtr[] del_topics,
                UIntPtr del_topic_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteTopics_result_topics(
                /* rd_kafka_DeleteTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
        );


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_NewPartitions_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic, 
                UIntPtr new_total_cnt,
                StringBuilder errstr, UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_NewPartitions_destroy(
                /* rd_kafka_NewPartitions_t * */ IntPtr new_parts);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewPartitions_set_replica_assignment(
                /* rd_kafka_NewPartitions_t * */ IntPtr new_parts,
                int new_partition_idx,
                int[] broker_ids,
                UIntPtr broker_id_cnt,
                StringBuilder errstr,
                UIntPtr errstr_size);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreatePartitions(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_NewPartitions_t ***/ IntPtr[] new_parts,
                UIntPtr new_parts_cnt,
                /* const rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_result_t ** */ IntPtr rd_kafka_CreatePartitions_result_topics(
                /* const rd_kafka_CreatePartitions_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigSource_name(
                ConfigSource configsource);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_name(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_value (
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfigSource rd_kafka_ConfigEntry_source(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_read_only(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_default(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_sensitive(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_synonym (
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigEntry_synonyms(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry,
                /* size_t * */ out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ResourceType_name(
                ResourceType restype);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t * */ IntPtr rd_kafka_ConfigResource_new(
                ResourceType restype,
                [MarshalAs(UnmanagedType.LPStr)] string resname); // todo: string?

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ConfigResource_destroy(
                /* rd_kafka_ConfigResource_t * */ IntPtr config);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_add_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name, 
                [MarshalAs(UnmanagedType.LPStr)] string value);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_set_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name, 
                [MarshalAs(UnmanagedType.LPStr)] string value);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_delete_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigResource_configs(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                /* size_t * */ out UIntPtr cntp);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourceType rd_kafka_ConfigResource_type(
                /* rd_kafka_ConfigResource_t * */ IntPtr config);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* char * */ IntPtr rd_kafka_ConfigResource_name(
                /* rd_kafka_ConfigResource_t * */ IntPtr config);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_error(
                /* rd_kafka_ConfigResource_t * */ IntPtr config);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigResource_error_string(
                /* rd_kafka_ConfigResource_t * */ IntPtr config);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConfigs (
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_AlterConfigs_result_resources(
                /* rd_kafka_AlterConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeConfigs (
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ***/ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_DescribeConfigs_result_resources(
                /* rd_kafka_DescribeConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp);



        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_topic_result_error(IntPtr topicres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_error_string(IntPtr topicres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_name(IntPtr topicres);


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
        internal static extern Librdkafka.EventType rd_kafka_event_type(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_opaque(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_event_error(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_error_string(IntPtr rkev);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_topic_partition_list(IntPtr rkev);


        //
        // error_t
        //

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_error_code(IntPtr error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_string(IntPtr error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_is_fatal(IntPtr error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_is_retriable(IntPtr error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_txn_requires_abort(IntPtr error);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_error_destroy(IntPtr error);
    }
}