// Copyright 2016-2023 Confluent Inc.
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
    internal class NativeMethods_Alpine
    {
#if NET462
        public const string DllName = "alpine-librdkafka.so";
#else
        public const string DllName = "alpine-librdkafka";
#endif

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
        internal static extern IntPtr rd_kafka_message_errstr(
                /* rd_kafka_message_t * */ IntPtr rkmessage);

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
        internal static extern int rd_kafka_message_leader_epoch(
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
        internal static extern SafeTopicConfigHandle rd_kafka_default_topic_conf_dup(SafeKafkaHandle rk);

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
        internal static extern SafeTopicConfigHandle rd_kafka_conf_get_default_topic_conf(
                SafeConfigHandle conf);

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
        internal static extern SafeTopicConfigHandle rd_kafka_topic_conf_dup(
                SafeTopicConfigHandle conf);

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
        internal static extern void rd_kafka_topic_conf_set_opaque(
                IntPtr topic_conf, IntPtr opaque);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                IntPtr topic_conf, Librdkafka.PartitionerDelegate partitioner_cb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern bool rd_kafka_topic_partition_available(
                IntPtr rkt, int partition);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_topic_partition_get_leader_epoch(
                IntPtr rkt);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_partition_set_leader_epoch(
                IntPtr rkt, int leader_epoch);

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
        internal static extern /* rd_kafka_Uuid_t * */IntPtr rd_kafka_Uuid_new(
                long most_significant_bits,
                long least_significant_bits
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* char * */IntPtr rd_kafka_Uuid_base64str(IntPtr uuid);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern long rd_kafka_Uuid_most_significant_bits(IntPtr uuid);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern long rd_kafka_Uuid_least_significant_bits(IntPtr uuid);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_Uuid_destroy(IntPtr uuid);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicHandle rd_kafka_topic_new(
                IntPtr rk, IntPtr topic,
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
        internal static extern IntPtr rd_kafka_incremental_assign(IntPtr rk,
                    /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_incremental_unassign(IntPtr rk,
                      /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_assignment_lost(IntPtr rk);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_rebalance_protocol(IntPtr rk);

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
        internal static extern IntPtr rd_kafka_seek_partitions(
                IntPtr rkt, IntPtr partitions, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_committed(
                IntPtr rk, IntPtr partitions, IntPtr timeout_ms);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_position(
                IntPtr rk, IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern unsafe IntPtr rd_kafka_produceva(
                IntPtr rk,
                rd_kafka_vu* vus,
                IntPtr size);

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
        internal static extern IntPtr rd_kafka_sasl_set_credentials(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string username,
                [MarshalAs(UnmanagedType.LPStr)] string password);

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
        internal static extern IntPtr rd_kafka_AdminOptions_set_require_stable_offsets(
                        IntPtr options,
                        IntPtr true_or_false);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_set_include_authorized_operations(
                        IntPtr options,
                        IntPtr true_or_false);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_set_isolation_level(
                        IntPtr options,
                        IntPtr isolation_level);

       [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_set_match_consumer_group_states(
                        IntPtr options,
                        ConsumerGroupState[] states,
                        UIntPtr statesCnt);

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
        internal static extern /* rd_kafka_DeleteGroup_t * */ IntPtr rd_kafka_DeleteGroup_new(
                [MarshalAs(UnmanagedType.LPStr)] string group
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteGroup_destroy(
                /* rd_kafka_DeleteGroup_t * */ IntPtr del_group);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteGroups(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteGroup_t ** */ IntPtr[] del_groups,
                UIntPtr del_group_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteGroups_result_groups(
                /* rd_kafka_DeleteGroups_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
        );


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteRecords_t * */ IntPtr rd_kafka_DeleteRecords_new(
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteRecords_destroy(
                /* rd_kafka_DeleteRecords_t * */ IntPtr del_topic);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteRecords(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteRecords_t ** */ IntPtr[] del_records,
                UIntPtr del_records_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_list_t * */ IntPtr rd_kafka_DeleteRecords_result_offsets(
                /* rd_kafka_DeleteRecords_result_t * */ IntPtr result);


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
        internal static extern /* rd_kafka_error_t * */ IntPtr rd_kafka_ConfigResource_add_incremental_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                /* rd_kafka_AlterConfigOpType_t */ AlterConfigOpType optype,
                [MarshalAs(UnmanagedType.LPStr)] string value);

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
        internal static extern void rd_kafka_IncrementalAlterConfigs(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu);
                
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_IncrementalAlterConfigs_result_resources(
                /* rd_kafka_IncrementalAlterConfigs_result_t * */ IntPtr result,
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
        internal static extern IntPtr rd_kafka_AclBinding_new(
                        /* rd_kafka_ResourceType_t */ ResourceType restype,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string name,
                        /* rd_kafka_ResourcePatternType_t */ ResourcePatternType resource_pattern_type,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string principal,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string host,
                        /* rd_kafka_AclOperation_t */ AclOperation operation,
                        /* rd_kafka_AclPermissionType_t */ AclPermissionType permission_type,
                        /* char * */ StringBuilder errstr,
                        /* size_t */ UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBindingFilter_new(
                        /* rd_kafka_ResourceType_t */ ResourceType restype,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string name,
                        /* rd_kafka_ResourcePatternType_t */ ResourcePatternType resource_pattern_type,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string principal,
                        /* const char * */[MarshalAs(UnmanagedType.LPStr)] string host,
                        /* rd_kafka_AclOperation_t */ AclOperation operation,
                        /* rd_kafka_AclPermissionType_t */ AclPermissionType permission_type,
                        /* char * */ StringBuilder errstr,
                        /* size_t */ UIntPtr errstr_size);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AclBinding_destroy(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourceType rd_kafka_AclBinding_restype(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_name(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourcePatternType rd_kafka_AclBinding_resource_pattern_type(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_principal(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_host(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern AclOperation rd_kafka_AclBinding_operation(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern AclPermissionType rd_kafka_AclBinding_permission_type(
                        /* rd_kafka_AclBinding_t * */ IntPtr acl_binding);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreateAcls(
                        /* rd_kafka_t * */ IntPtr rk,
                        /* rd_kafka_AclBinding_t ** */ IntPtr[] new_acls,
                        UIntPtr new_acls_cnt,
                        /* rd_kafka_AdminOptions_t * */ IntPtr options,
                        /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_CreateAcls_result_acls(
                        /* const rd_kafka_CreateAcls_result_t * */ IntPtr result,
                        /* size_t * */ out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_acl_result_error(
                        /* const rd_kafka_acl_result_t * */ IntPtr aclres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeAcls(
                        /* rd_kafka_t * */ IntPtr rk,
                        /* rd_kafka_AclBindingFilter_t * */ IntPtr acl_filter,
                        /* rd_kafka_AdminOptions_t * */ IntPtr options,
                        /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeAcls_result_acls(
                        /* const rd_kafka_DescribeAcls_result_t * */ IntPtr result,
                        /* size_t * */ out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteAcls(
                        /* rd_kafka_t * */ IntPtr rk,
                        /* rd_kafka_AclBindingFilter_t ** */ IntPtr[] del_acls,
                        UIntPtr del_acls_cnt,
                        /* rd_kafka_AdminOptions_t * */ IntPtr options,
                        /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_responses(
                        /* rd_kafka_DeleteAcls_result_t * */ IntPtr result,
                        /* size_t * */ out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_response_error(
                        /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_response_matching_acls(
                        /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response,
                        /* size_t * */ out UIntPtr matching_acls_cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteConsumerGroupOffsets_t */ IntPtr rd_kafka_DeleteConsumerGroupOffsets_new(
                        [MarshalAs(UnmanagedType.LPStr)] string group,
                        /* rd_kafka_topic_partition_list_t * */ IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteConsumerGroupOffsets_destroy(
                        /* rd_kafka_DeleteConsumerGroupOffsets_t * */ IntPtr del_grp_offsets);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteConsumerGroupOffsets(
                       /* rd_kafka_t * */ IntPtr rk,
                       /* rd_kafka_DeleteConsumerGroupOffsets_t ** */ IntPtr[] del_grp_offsets,
                       UIntPtr del_grp_offsets_cnt,
                       /* rd_kafka_AdminOptions_t * */ IntPtr options,
                       /* rd_kafka_queue_t * */ IntPtr rkqu);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteConsumerGroupOffsets_result_groups(
                /* rd_kafka_DeleteConsumerGroupOffsets_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group, IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroupOffsets_destroy(IntPtr groupPartitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroupOffsets_result_groups(
                IntPtr resultResponse, out UIntPtr groupsTopicPartitionsCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] listGroupsPartitions,
                UIntPtr listGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group, IntPtr partitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConsumerGroupOffsets_destroy(IntPtr groupPartitions);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterConsumerGroupOffsets_result_groups(
                IntPtr resultResponse, out UIntPtr groupsTopicPartitionsCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] alterGroupsPartitions,
                UIntPtr alterGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroups(
                IntPtr handle,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupListing_group_id(IntPtr grplist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupListing_is_simple_consumer_group(IntPtr grplist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConsumerGroupState rd_kafka_ConsumerGroupListing_state(IntPtr grplist);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroups_result_valid(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroups_result_errors(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeConsumerGroups(
                IntPtr handle,
                [MarshalAs(UnmanagedType.LPArray)] string[] groups,
                UIntPtr groupsCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeConsumerGroups_result_groups(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_group_id(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_error(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_partition_assignor(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConsumerGroupState rd_kafka_ConsumerGroupDescription_state(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_coordinator(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_authorized_operations(IntPtr grpdesc, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_member_count(IntPtr grpdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_member(IntPtr grpdesc, IntPtr idx);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_client_id(IntPtr member);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_group_instance_id(IntPtr member);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_consumer_id(IntPtr member);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_host(IntPtr member);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_assignment(IntPtr member);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberAssignment_partitions(IntPtr assignment);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_id(IntPtr node);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_host(IntPtr node);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_port(IntPtr node);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_rack(IntPtr node);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_topic_result_error(IntPtr topicres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_error_string(IntPtr topicres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_name(IntPtr topicres);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_name(IntPtr groupres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_error(IntPtr groupres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_partitions(IntPtr groupres);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeUserScramCredentials(
                IntPtr handle,
                [MarshalAs(UnmanagedType.LPArray)] string[] users,
                UIntPtr usersCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AlterUserScramCredentials(
                IntPtr handle,
                IntPtr[] alterations,
                UIntPtr alterationsCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialDeletion_new(
                string user,
                ScramMechanism mechanism);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialUpsertion_new(
                string user,
                ScramMechanism mechanism,
                int iterations,
                byte[] password,
                IntPtr passwordSize,
                byte[] salt,
                IntPtr saltSize);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_UserScramCredentialAlteration_destroy(
                IntPtr alteration);
             
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeUserScramCredentials_result_descriptions(
                IntPtr event_result,
                out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_user(IntPtr description);


        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_error(IntPtr description);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(IntPtr description);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(IntPtr description, int i);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ScramMechanism rd_kafka_ScramCredentialInfo_mechanism(IntPtr scramcredentialinfo);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_ScramCredentialInfo_iterations(IntPtr scramcredentialinfo);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_responses(
                IntPtr event_result,
                out UIntPtr cntp);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_response_user(IntPtr element);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_response_error(IntPtr element);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListOffsets(IntPtr handle, IntPtr topic_partition_list, IntPtr options, IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListOffsets_result_infos(IntPtr resultPtr, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern long rd_kafka_ListOffsetsResultInfo_timestamp(IntPtr element);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListOffsetsResultInfo_topic_partition(IntPtr element);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeTopics(
                IntPtr handle,
                IntPtr topicCollection,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeTopics_result_topics(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicCollection_of_topic_names([MarshalAs(UnmanagedType.LPArray)] string[] topics,
                UIntPtr topicsCnt);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_TopicCollection_destroy(IntPtr topic_collection);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_error(IntPtr topicdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_name(IntPtr topicdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_topic_id(IntPtr topicdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_partitions(IntPtr topicdesc, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_is_internal(IntPtr topicdesc);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicDescription_authorized_operations(IntPtr topicdesc, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicPartitionInfo_isr(IntPtr topic_partition_info, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicPartitionInfo_leader(IntPtr topic_partition_info);
        
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_TopicPartitionInfo_partition(IntPtr topic_partition_info);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_TopicPartitionInfo_replicas(IntPtr topic_partition_info, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeCluster(
                IntPtr handle,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeCluster_result_nodes(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeCluster_result_authorized_operations(IntPtr result, out UIntPtr cntp);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeCluster_result_controller(IntPtr result);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeCluster_result_cluster_id(IntPtr result);

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
