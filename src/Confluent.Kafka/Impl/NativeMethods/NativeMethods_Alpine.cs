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
using Confluent.Kafka.Admin;
#if NET7_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

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
#if NET7_0_OR_GREATER
    internal partial class NativeMethods_Alpine
#else
    internal class NativeMethods_Alpine
#endif
    {
#if NET462
        public const string DllName = "alpine-librdkafka.so";
#else
        public const string DllName = "alpine-librdkafka";
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_version();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_version();
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_version_str();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_version_str();
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_get_debug_contexts();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_get_debug_contexts();
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_err2str(ErrorCode err);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_err2str(ErrorCode err);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_last_error();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_last_error();
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_fatal_error(
                IntPtr rk,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_message_errstr(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_message_errstr(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_topic_partition_list_t * */ IntPtr rd_kafka_topic_partition_list_new(IntPtr size);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_list_t * */ IntPtr rd_kafka_topic_partition_list_new(IntPtr size);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_partition_list_destroy(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rkparlist
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_partition_list_destroy(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rkparlist
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_topic_partition_t * */ IntPtr rd_kafka_topic_partition_list_add(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rktparlist,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_t * */ IntPtr rd_kafka_topic_partition_list_add(
                /* rd_kafka_topic_partition_list_t * */ IntPtr rktparlist,
                string topic, 
                int partition
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_headers_t * */ IntPtr rd_kafka_headers_new(IntPtr size);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_headers_t * */ IntPtr rd_kafka_headers_new(IntPtr size);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_headers_destroy(
                /* rd_kafka_headers_t * */ IntPtr hdrs
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_headers_destroy(
                /* rd_kafka_headers_t * */ IntPtr hdrs
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_header_add(
                /* rd_kafka_headers_t * */ IntPtr hdrs,
                /* const char * */ IntPtr name,
                /* ssize_t */ IntPtr name_size,
                /* const void * */ IntPtr value,
                /* ssize_t */ IntPtr value_size
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_header_add(
                /* rd_kafka_headers_t * */ IntPtr hdrs,
                /* const char * */ IntPtr name,
                /* ssize_t */ IntPtr name_size,
                /* const void * */ IntPtr value,
                /* ssize_t */ IntPtr value_size
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_header_get_all(
                /* const rd_kafka_headers_t * */ IntPtr hdrs,
                /* const size_t */ IntPtr idx,
                /* const char ** */ out IntPtr namep,
                /* const void ** */ out IntPtr valuep,
                /* size_t * */ out IntPtr sizep
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_header_get_all(
                /* const rd_kafka_headers_t * */ IntPtr hdrs,
                /* const size_t */ IntPtr idx,
                /* const char ** */ out IntPtr namep,
                /* const void ** */ out IntPtr valuep,
                /* size_t * */ out IntPtr sizep
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* int64_t */ long rd_kafka_message_timestamp(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_timestamp_type_t * */ out IntPtr tstype
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* int64_t */ long rd_kafka_message_timestamp(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_timestamp_type_t * */ out IntPtr tstype
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_message_headers(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_headers_t * */ out IntPtr hdrs
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_message_headers(
                /* rd_kafka_message_t * */ IntPtr rkmessage,
                /* r_kafka_headers_t * */ out IntPtr hdrs
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial PersistenceStatus rd_kafka_message_status(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern PersistenceStatus rd_kafka_message_status(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_message_leader_epoch(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_message_leader_epoch(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_message_destroy(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_message_destroy(
                /* rd_kafka_message_t * */ IntPtr rkmessage
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeConfigHandle rd_kafka_conf_new();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeConfigHandle rd_kafka_conf_new();
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_destroy(IntPtr conf);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_destroy(IntPtr conf);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_conf_dup(IntPtr conf);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_conf_dup(IntPtr conf);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeTopicConfigHandle rd_kafka_default_topic_conf_dup(SafeKafkaHandle rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicConfigHandle rd_kafka_default_topic_conf_dup(SafeKafkaHandle rk);
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_conf_set(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_dr_msg_cb(
                IntPtr conf,
                Librdkafka.DeliveryReportDelegate dr_msg_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_dr_msg_cb(
                IntPtr conf,
                Librdkafka.DeliveryReportDelegate dr_msg_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf,
                Librdkafka.RebalanceDelegate rebalance_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_rebalance_cb(
                IntPtr conf,
                Librdkafka.RebalanceDelegate rebalance_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf,
                Librdkafka.CommitDelegate commit_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_offset_commit_cb(
                IntPtr conf,
                Librdkafka.CommitDelegate commit_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_error_cb(
                IntPtr conf,
                Librdkafka.ErrorDelegate error_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_error_cb(
                IntPtr conf,
                Librdkafka.ErrorDelegate error_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_log_cb(
                IntPtr conf,
                Librdkafka.LogDelegate log_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_log_cb(
                IntPtr conf,
                Librdkafka.LogDelegate log_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_oauthbearer_token_refresh_cb(
                IntPtr conf,
                Librdkafka.OAuthBearerTokenRefreshDelegate oauthbearer_token_refresh_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_oauthbearer_token_refresh_cb(
                IntPtr conf, 
                Librdkafka.OAuthBearerTokenRefreshDelegate oauthbearer_token_refresh_cb
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_oauthbearer_set_token(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string token_value,
                long md_lifetime_ms,
                [MarshalAs(UnmanagedType.LPStr)] string md_principal_name,
                [MarshalAs(UnmanagedType.LPArray)] string[] extensions,
                UIntPtr extension_size,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_oauthbearer_set_token_failure(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string errstr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_oauthbearer_set_token_failure(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string errstr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_stats_cb(
                IntPtr conf,
                Librdkafka.StatsDelegate stats_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_stats_cb(
                IntPtr conf,
                Librdkafka.StatsDelegate stats_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_set_default_topic_conf(
                IntPtr conf,
                IntPtr tconf
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_set_default_topic_conf(
                IntPtr conf, 
                IntPtr tconf
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeTopicConfigHandle rd_kafka_conf_get_default_topic_conf(
                SafeConfigHandle conf
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicConfigHandle rd_kafka_conf_get_default_topic_conf(
                SafeConfigHandle conf
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_conf_get(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                StringBuilder dest,
                ref UIntPtr dest_size
                );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_topic_conf_get(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                StringBuilder dest,
                ref UIntPtr dest_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* const char ** */ IntPtr rd_kafka_conf_dump(
                IntPtr conf,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char ** */ IntPtr rd_kafka_conf_dump(
                IntPtr conf,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* const char ** */ IntPtr rd_kafka_topic_conf_dump(
                IntPtr conf,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char ** */ IntPtr rd_kafka_topic_conf_dump(
                IntPtr conf, 
                out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_conf_dump_free(
                /* const char ** */ IntPtr arr,
                UIntPtr cnt
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_conf_dump_free(
                /* const char ** */ IntPtr arr,
                UIntPtr cnt
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeTopicConfigHandle rd_kafka_topic_conf_new();
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicConfigHandle rd_kafka_topic_conf_new();
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeTopicConfigHandle rd_kafka_topic_conf_dup(
                SafeTopicConfigHandle conf
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicConfigHandle rd_kafka_topic_conf_dup(
                SafeTopicConfigHandle conf
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_conf_destroy(IntPtr conf);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_destroy(IntPtr conf);
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfRes rd_kafka_topic_conf_set(
                IntPtr conf,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_conf_set_opaque(
                IntPtr topic_conf,
                IntPtr opaque
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_set_opaque(
                IntPtr topic_conf,
                IntPtr opaque
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_conf_set_partitioner_cb(
                IntPtr topic_conf,
                Librdkafka.PartitionerDelegate partitioner_cb
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_conf_set_partitioner_cb(
                IntPtr topic_conf,
                Librdkafka.PartitionerDelegate partitioner_cb
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool rd_kafka_topic_partition_available(
                IntPtr rkt,
                int partition
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern bool rd_kafka_topic_partition_available(
                IntPtr rkt,
                int partition
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_topic_partition_get_leader_epoch(
                IntPtr rkt
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_topic_partition_get_leader_epoch(
                IntPtr rkt
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_partition_set_leader_epoch(
                IntPtr rkt,
                int leader_epoch
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_partition_set_leader_epoch(
                IntPtr rkt,
                int leader_epoch
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_init_transactions(
                IntPtr rk,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_init_transactions(
                IntPtr rk,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_begin_transaction(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_begin_transaction(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_commit_transaction(
                IntPtr rk,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_commit_transaction(
                IntPtr rk,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_abort_transaction(
                IntPtr rk,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_abort_transaction(
                IntPtr rk,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_send_offsets_to_transaction(
                IntPtr rk,
                IntPtr offsets,
                IntPtr consumer_group_metadata,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_send_offsets_to_transaction(
                IntPtr rk,
                IntPtr offsets, 
                IntPtr consumer_group_metadata,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_consumer_group_metadata(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_consumer_group_metadata_destroy(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_consumer_group_metadata_destroy(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_consumer_group_metadata_write(
                /* rd_kafka_consumer_group_metadata_t * */IntPtr cgmd,
                /* const void ** */ out IntPtr valuep,
                /* size_t * */ out IntPtr sizep
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata_write(
                /* rd_kafka_consumer_group_metadata_t * */IntPtr cgmd,
                /* const void ** */ out IntPtr valuep,
                /* size_t * */ out IntPtr sizep
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_consumer_group_metadata_read(
                /* rd_kafka_consumer_group_metadata_t ** */ out IntPtr cgmdp,
                byte[] buffer,
                IntPtr size
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_consumer_group_metadata_read(
                /* rd_kafka_consumer_group_metadata_t ** */ out IntPtr cgmdp,
                byte[] buffer,
                IntPtr size
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeKafkaHandle rd_kafka_new(
                        RdKafkaType type,
                        IntPtr conf,
                        StringBuilder errstr,
                        UIntPtr errstr_size
                        );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_destroy(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_destroy(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_destroy_flags(
                IntPtr rk,
                IntPtr flags
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_destroy_flags(
                IntPtr rk,
                IntPtr flags
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* const char * */ IntPtr rd_kafka_name(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_name(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* char * */ IntPtr rd_kafka_memberid(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* char * */ IntPtr rd_kafka_memberid(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial SafeTopicHandle rd_kafka_topic_new(
                IntPtr rk,
                IntPtr topic,
                /* rd_kafka_topic_conf_t * */ IntPtr conf
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern SafeTopicHandle rd_kafka_topic_new(
                IntPtr rk,
                IntPtr topic,
                /* rd_kafka_topic_conf_t * */ IntPtr conf
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_topic_destroy(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_topic_destroy(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* const char * */ IntPtr rd_kafka_topic_name(IntPtr rkt);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* const char * */ IntPtr rd_kafka_topic_name(IntPtr rkt);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_poll_set_consumer(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_poll_set_consumer(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_poll(IntPtr rk, IntPtr timeout_ms);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_poll(IntPtr rk, IntPtr timeout_ms);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_query_watermark_offsets(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition,
                out long low,
                out long high,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_query_watermark_offsets(IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition,
                out long low,
                out long high,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_get_watermark_offsets(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition,
                out long low,
                out long high
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_get_watermark_offsets(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                int partition, 
                out long low, 
                out long high
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_offsets_for_times(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_offsets_for_times(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_mem_free(
                IntPtr rk,
                IntPtr ptr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_mem_free(
                IntPtr rk,
                IntPtr ptr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_subscribe(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr topics
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_subscribe(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr topics
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_unsubscribe(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_unsubscribe(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_subscription(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_subscription(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_message_t * */ IntPtr rd_kafka_consumer_poll(
                IntPtr rk,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_message_t * */ IntPtr rd_kafka_consumer_poll(
                IntPtr rk,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_consumer_close(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_consumer_close(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_assign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_assign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_incremental_assign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_incremental_assign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_incremental_unassign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_incremental_unassign(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_assignment_lost(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_assignment_lost(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_rebalance_protocol(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_rebalance_protocol(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_assignment(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_assignment(
                IntPtr rk,
                /* rd_kafka_topic_partition_list_t ** */ out IntPtr topics
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_offsets_store(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_offsets_store(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_commit(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                [MarshalAs(UnmanagedType.Bool)] bool async
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_commit(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                bool async
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_commit_queue(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                /* rd_kafka_queue_t * */ IntPtr rkqu,
                /* offset_commit_cb * */ Librdkafka.CommitDelegate cb,
                /* void * */ IntPtr opaque
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_commit_queue(
                IntPtr rk,
                /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                /* rd_kafka_queue_t * */ IntPtr rkqu,
                /* offset_commit_cb * */ Librdkafka.CommitDelegate cb,
                /* void * */ IntPtr opaque
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_pause_partitions(
                IntPtr rk,
                IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_pause_partitions(
                IntPtr rk, 
                IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_resume_partitions(
                IntPtr rk,
                IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_resume_partitions(
                IntPtr rk, 
                IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_seek(
                IntPtr rkt,
                int partition,
                long offset,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_seek(
                IntPtr rkt,
                int partition, 
                long offset, 
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_seek_partitions(
                IntPtr rkt,
                IntPtr partitions,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_seek_partitions(
                IntPtr rkt,
                IntPtr partitions,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_committed(
                IntPtr rk,
                IntPtr partitions,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_committed(
                IntPtr rk,
                IntPtr partitions,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_position(
                IntPtr rk,
                IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_position(
                IntPtr rk, 
                IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal unsafe static partial IntPtr rd_kafka_produceva(
                IntPtr rk,
                rd_kafka_vu* vus,
                IntPtr size
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal unsafe static extern IntPtr rd_kafka_produceva(
                IntPtr rk,
                rd_kafka_vu* vus,
                IntPtr size
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_flush(
            IntPtr rk,
            IntPtr timeout_ms
            );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_flush(
            IntPtr rk,
            IntPtr timeout_ms
            );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_metadata(
            IntPtr rk,
            [MarshalAs(UnmanagedType.Bool)] bool all_topics,
            /* rd_kafka_topic_t * */ IntPtr only_rkt,
            /* const struct rd_kafka_metadata ** */ out IntPtr metadatap,
            IntPtr timeout_ms
            );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_metadata(
            IntPtr rk, 
            bool all_topics,
            /* rd_kafka_topic_t * */ IntPtr only_rkt,
            /* const struct rd_kafka_metadata ** */ out IntPtr metadatap,
            IntPtr timeout_ms
            );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_metadata_destroy(
                /* const struct rd_kafka_metadata * */ IntPtr metadata
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_metadata_destroy(
                /* const struct rd_kafka_metadata * */ IntPtr metadata
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_list_groups(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string group,
                out IntPtr grplistp,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_list_groups(
                IntPtr rk,
                string group,
                out IntPtr grplistp,
                IntPtr timeout_ms
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_group_list_destroy(
                IntPtr grplist
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_group_list_destroy(
                IntPtr grplist
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_brokers_add(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string brokerlist
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_brokers_add(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string brokerlist
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_sasl_set_credentials(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string username,
                [MarshalAs(UnmanagedType.LPStr)] string password
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_sasl_set_credentials(
                IntPtr rk,
                [MarshalAs(UnmanagedType.LPStr)] string username,
                [MarshalAs(UnmanagedType.LPStr)] string password
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_outq_len(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_outq_len(IntPtr rk);
#endif

        //
        // Admin API
        //

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AdminOptions_new(
                IntPtr rk,
                Librdkafka.AdminOp op
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_new(
                IntPtr rk,
                Librdkafka.AdminOp op
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AdminOptions_destroy(IntPtr options);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AdminOptions_destroy(IntPtr options);
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_request_timeout(
                        IntPtr options,
                        IntPtr timeout_ms,
                        StringBuilder errstr,
                        UIntPtr errstr_size
                        );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_operation_timeout(
                IntPtr options,
                IntPtr timeout_ms,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_validate_only(
                IntPtr options,
                IntPtr true_or_false,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_incremental(
                IntPtr options,
                IntPtr true_or_false,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AdminOptions_set_broker(
                IntPtr options,
                int broker_id,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AdminOptions_set_opaque(
                IntPtr options,
                IntPtr opaque
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AdminOptions_set_opaque(
                IntPtr options,
                IntPtr opaque
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AdminOptions_set_require_stable_offsets(
                IntPtr options,
                IntPtr true_or_false
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_set_require_stable_offsets(
                IntPtr options,
                IntPtr true_or_false
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AdminOptions_set_match_consumer_group_states(
                IntPtr options,
                ConsumerGroupState[] states,
                UIntPtr statesCnt
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AdminOptions_set_match_consumer_group_states(
                IntPtr options,
                ConsumerGroupState[] states,
                UIntPtr statesCnt
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_NewTopic_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                IntPtr num_partitions,
                IntPtr replication_factor,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_NewTopic_destroy(
                IntPtr new_topic
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_NewTopic_destroy(
                IntPtr new_topic
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewTopic_set_replica_assignment(
                IntPtr new_topic,
                int partition,
                int[] broker_ids,
                UIntPtr broker_id_cnt,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_NewTopic_set_config(
                IntPtr new_topic,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewTopic_set_config(
                IntPtr new_topic,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_CreateTopics(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_NewTopic_t ** */ IntPtr[] new_topics,
                UIntPtr new_topic_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreateTopics(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_NewTopic_t ** */ IntPtr[] new_topics,
                UIntPtr new_topic_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_CreateTopics_result_topics(
                /* rd_kafka_CreateTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_CreateTopics_result_topics(
                /* rd_kafka_CreateTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_DeleteTopic_t * */ IntPtr rd_kafka_DeleteTopic_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteTopic_t * */ IntPtr rd_kafka_DeleteTopic_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteTopic_destroy(
                /* rd_kafka_DeleteTopic_t * */ IntPtr del_topic
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteTopic_destroy(
                /* rd_kafka_DeleteTopic_t * */ IntPtr del_topic
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteTopics(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteTopic_t ** */ IntPtr[] del_topics,
                UIntPtr del_topic_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteTopics(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteTopic_t ** */ IntPtr[] del_topics,
                UIntPtr del_topic_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteTopics_result_topics(
                /* rd_kafka_DeleteTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteTopics_result_topics(
                /* rd_kafka_DeleteTopics_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_DeleteGroup_t * */ IntPtr rd_kafka_DeleteGroup_new(
                [MarshalAs(UnmanagedType.LPStr)] string group
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteGroup_t * */ IntPtr rd_kafka_DeleteGroup_new(
                [MarshalAs(UnmanagedType.LPStr)] string group
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteGroup_destroy(
                /* rd_kafka_DeleteGroup_t * */ IntPtr del_group
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteGroup_destroy(
                /* rd_kafka_DeleteGroup_t * */ IntPtr del_group
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteGroups(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteGroup_t ** */ IntPtr[] del_groups,
                UIntPtr del_group_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteGroups(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteGroup_t ** */ IntPtr[] del_groups,
                UIntPtr del_group_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteGroups_result_groups(
                /* rd_kafka_DeleteGroups_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteGroups_result_groups(
                /* rd_kafka_DeleteGroups_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_DeleteRecords_t * */ IntPtr rd_kafka_DeleteRecords_new(
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteRecords_t * */ IntPtr rd_kafka_DeleteRecords_new(
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteRecords_destroy(
                /* rd_kafka_DeleteRecords_t * */ IntPtr del_topic
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteRecords_destroy(
                /* rd_kafka_DeleteRecords_t * */ IntPtr del_topic
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteRecords(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteRecords_t ** */ IntPtr[] del_records,
                UIntPtr del_records_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteRecords(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteRecords_t ** */ IntPtr[] del_records,
                UIntPtr del_records_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_topic_partition_list_t * */ IntPtr rd_kafka_DeleteRecords_result_offsets(
                /* rd_kafka_DeleteRecords_result_t * */ IntPtr result
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_partition_list_t * */ IntPtr rd_kafka_DeleteRecords_result_offsets(
                /* rd_kafka_DeleteRecords_result_t * */ IntPtr result
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_NewPartitions_new(
                [MarshalAs(UnmanagedType.LPStr)] string topic,
                UIntPtr new_total_cnt,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_NewPartitions_destroy(
                /* rd_kafka_NewPartitions_t * */ IntPtr new_parts
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_NewPartitions_destroy(
                /* rd_kafka_NewPartitions_t * */ IntPtr new_parts
                );
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_NewPartitions_set_replica_assignment(
                /* rd_kafka_NewPartitions_t * */ IntPtr new_parts,
                int new_partition_idx,
                int[] broker_ids,
                UIntPtr broker_id_cnt,
                StringBuilder errstr,
                UIntPtr errstr_size
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_CreatePartitions(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_NewPartitions_t ***/ IntPtr[] new_parts,
                UIntPtr new_parts_cnt,
                /* const rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreatePartitions(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_NewPartitions_t ***/ IntPtr[] new_parts,
                UIntPtr new_parts_cnt,
                /* const rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_topic_result_t ** */ IntPtr rd_kafka_CreatePartitions_result_topics(
                /* const rd_kafka_CreatePartitions_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_topic_result_t ** */ IntPtr rd_kafka_CreatePartitions_result_topics(
                /* const rd_kafka_CreatePartitions_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigSource_name(
                ConfigSource configsource
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigSource_name(
                ConfigSource configsource
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_name(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_name(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_value(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_value (
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ConfigSource rd_kafka_ConfigEntry_source(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConfigSource rd_kafka_ConfigEntry_source(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_is_read_only(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_read_only(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_is_default(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_default(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_is_sensitive(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_sensitive(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigEntry_is_synonym(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigEntry_is_synonym (
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigEntry_synonyms(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigEntry_synonyms(
                /* rd_kafka_ConfigEntry_t * */ IntPtr entry,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ResourceType_name(
                ResourceType restype
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ResourceType_name(
                ResourceType restype
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigResource_t * */ IntPtr rd_kafka_ConfigResource_new(
                ResourceType restype,
                [MarshalAs(UnmanagedType.LPStr)] string resname // todo: string?
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t * */ IntPtr rd_kafka_ConfigResource_new(
                ResourceType restype,
                [MarshalAs(UnmanagedType.LPStr)] string resname // todo: string?
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_ConfigResource_destroy(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ConfigResource_destroy(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_ConfigResource_add_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_add_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_ConfigResource_set_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_set_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_ConfigResource_delete_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_delete_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_error_t * */ IntPtr rd_kafka_ConfigResource_add_incremental_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                /* rd_kafka_AlterConfigOpType_t */ AlterConfigOpType optype,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_error_t * */ IntPtr rd_kafka_ConfigResource_add_incremental_config(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                [MarshalAs(UnmanagedType.LPStr)] string name,
                /* rd_kafka_AlterConfigOpType_t */ AlterConfigOpType optype,
                [MarshalAs(UnmanagedType.LPStr)] string value
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigResource_configs(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigEntry_t ** */ IntPtr rd_kafka_ConfigResource_configs(
                /* rd_kafka_ConfigResource_t * */ IntPtr config,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ResourceType rd_kafka_ConfigResource_type(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourceType rd_kafka_ConfigResource_type(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* char * */ IntPtr rd_kafka_ConfigResource_name(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* char * */ IntPtr rd_kafka_ConfigResource_name(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_ConfigResource_error(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_ConfigResource_error(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConfigResource_error_string(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConfigResource_error_string(
                /* rd_kafka_ConfigResource_t * */ IntPtr config
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AlterConfigs(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConfigs (
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_AlterConfigs_result_resources(
                /* rd_kafka_AlterConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_AlterConfigs_result_resources(
                /* rd_kafka_AlterConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_IncrementalAlterConfigs(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_IncrementalAlterConfigs(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ** */ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_IncrementalAlterConfigs_result_resources(
                /* rd_kafka_IncrementalAlterConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_IncrementalAlterConfigs_result_resources(
                /* rd_kafka_IncrementalAlterConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DescribeConfigs(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ***/ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeConfigs (
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_ConfigResource_t ***/ IntPtr[] configs,
                UIntPtr config_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_DescribeConfigs_result_resources(
                /* rd_kafka_DescribeConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_ConfigResource_t ** */ IntPtr rd_kafka_DescribeConfigs_result_resources(
                /* rd_kafka_DescribeConfigs_result_t * */ IntPtr result,
                out UIntPtr cntp
                );
#endif

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
                        /* size_t */ UIntPtr errstr_size
                        );

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
                        /* size_t */ UIntPtr errstr_size
                        );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AclBinding_destroy(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AclBinding_destroy(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ResourceType rd_kafka_AclBinding_restype(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourceType rd_kafka_AclBinding_restype(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AclBinding_name(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_name(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ResourcePatternType rd_kafka_AclBinding_resource_pattern_type(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ResourcePatternType rd_kafka_AclBinding_resource_pattern_type(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AclBinding_principal(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_principal(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AclBinding_host(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AclBinding_host(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial AclOperation rd_kafka_AclBinding_operation(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern AclOperation rd_kafka_AclBinding_operation(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial AclPermissionType rd_kafka_AclBinding_permission_type(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern AclPermissionType rd_kafka_AclBinding_permission_type(
                /* rd_kafka_AclBinding_t * */ IntPtr acl_binding
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_CreateAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBinding_t ** */ IntPtr[] new_acls,
                UIntPtr new_acls_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_CreateAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBinding_t ** */ IntPtr[] new_acls,
                UIntPtr new_acls_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_CreateAcls_result_acls(
                /* const rd_kafka_CreateAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_CreateAcls_result_acls(
                /* const rd_kafka_CreateAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_acl_result_error(
            /* const rd_kafka_acl_result_t * */ IntPtr aclres
            );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_acl_result_error(
            /* const rd_kafka_acl_result_t * */ IntPtr aclres
            );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DescribeAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBindingFilter_t * */ IntPtr acl_filter,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBindingFilter_t * */ IntPtr acl_filter,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DescribeAcls_result_acls(
                /* const rd_kafka_DescribeAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeAcls_result_acls(
                /* const rd_kafka_DescribeAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBindingFilter_t ** */ IntPtr[] del_acls,
                UIntPtr del_acls_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteAcls(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_AclBindingFilter_t ** */ IntPtr[] del_acls,
                UIntPtr del_acls_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteAcls_result_responses(
                /* rd_kafka_DeleteAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_responses(
                /* rd_kafka_DeleteAcls_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteAcls_result_response_error(
                /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_response_error(
                /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteAcls_result_response_matching_acls(
                /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response,
                /* size_t * */ out UIntPtr matching_acls_cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteAcls_result_response_matching_acls(
                /* rd_kafka_DeleteAcls_result_response_t * */ IntPtr result_response,
                /* size_t * */ out UIntPtr matching_acls_cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial /* rd_kafka_DeleteConsumerGroupOffsets_t */ IntPtr rd_kafka_DeleteConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern /* rd_kafka_DeleteConsumerGroupOffsets_t */ IntPtr rd_kafka_DeleteConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteConsumerGroupOffsets_destroy(
                /* rd_kafka_DeleteConsumerGroupOffsets_t * */ IntPtr del_grp_offsets
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteConsumerGroupOffsets_destroy(
                /* rd_kafka_DeleteConsumerGroupOffsets_t * */ IntPtr del_grp_offsets
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_DeleteConsumerGroupOffsets(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteConsumerGroupOffsets_t ** */ IntPtr[] del_grp_offsets,
                UIntPtr del_grp_offsets_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DeleteConsumerGroupOffsets(
                /* rd_kafka_t * */ IntPtr rk,
                /* rd_kafka_DeleteConsumerGroupOffsets_t ** */ IntPtr[] del_grp_offsets,
                UIntPtr del_grp_offsets_cnt,
                /* rd_kafka_AdminOptions_t * */ IntPtr options,
                /* rd_kafka_queue_t * */ IntPtr rkqu
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DeleteConsumerGroupOffsets_result_groups(
                /* rd_kafka_DeleteConsumerGroupOffsets_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DeleteConsumerGroupOffsets_result_groups(
                /* rd_kafka_DeleteConsumerGroupOffsets_result_t * */ IntPtr result,
                /* size_t * */ out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ListConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_ListConsumerGroupOffsets_destroy(IntPtr groupPartitions);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroupOffsets_destroy(IntPtr groupPartitions);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ListConsumerGroupOffsets_result_groups(
                IntPtr resultResponse,
                out UIntPtr groupsTopicPartitionsCount
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroupOffsets_result_groups(
                IntPtr resultResponse,
                out UIntPtr groupsTopicPartitionsCount
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_ListConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] listGroupsPartitions,
                UIntPtr listGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] listGroupsPartitions,
                UIntPtr listGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AlterConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                IntPtr partitions
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterConsumerGroupOffsets_new(
                [MarshalAs(UnmanagedType.LPStr)] string group,
                IntPtr partitions
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AlterConsumerGroupOffsets_destroy(IntPtr groupPartitions);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConsumerGroupOffsets_destroy(IntPtr groupPartitions);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AlterConsumerGroupOffsets_result_groups(
                IntPtr resultResponse,
                out UIntPtr groupsTopicPartitionsCount
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterConsumerGroupOffsets_result_groups(
                IntPtr resultResponse,
                out UIntPtr groupsTopicPartitionsCount
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_AlterConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] alterGroupsPartitions,
                UIntPtr alterGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_AlterConsumerGroupOffsets(
                IntPtr handle,
                IntPtr[] alterGroupsPartitions,
                UIntPtr alterGroupsPartitionsSize,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_ListConsumerGroups(
                IntPtr handle,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_ListConsumerGroups(
                IntPtr handle,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupListing_group_id(IntPtr grplist);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupListing_group_id(IntPtr grplist);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupListing_is_simple_consumer_group(IntPtr grplist);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupListing_is_simple_consumer_group(IntPtr grplist);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ConsumerGroupState rd_kafka_ConsumerGroupListing_state(IntPtr grplist);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConsumerGroupState rd_kafka_ConsumerGroupListing_state(IntPtr grplist);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ListConsumerGroups_result_valid(IntPtr result, out UIntPtr cntp);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroups_result_valid(IntPtr result, out UIntPtr cntp);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ListConsumerGroups_result_errors(IntPtr result, out UIntPtr cntp);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ListConsumerGroups_result_errors(IntPtr result, out UIntPtr cntp);
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeConsumerGroups(
                        IntPtr handle,
                        [MarshalAs(UnmanagedType.LPArray)] string[] groups,
                        UIntPtr groupsCnt,
                        IntPtr optionsPtr,
                        IntPtr resultQueuePtr
                        );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DescribeConsumerGroups_result_groups(IntPtr result, out UIntPtr cntp);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeConsumerGroups_result_groups(IntPtr result, out UIntPtr cntp);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_group_id(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_group_id(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_error(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_error(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_partition_assignor(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_partition_assignor(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ConsumerGroupState rd_kafka_ConsumerGroupDescription_state(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ConsumerGroupState rd_kafka_ConsumerGroupDescription_state(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_coordinator(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_coordinator(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_member_count(IntPtr grpdesc);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_member_count(IntPtr grpdesc);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_ConsumerGroupDescription_member(IntPtr grpdesc, IntPtr idx);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_ConsumerGroupDescription_member(IntPtr grpdesc, IntPtr idx);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberDescription_client_id(IntPtr member);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_client_id(IntPtr member);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberDescription_group_instance_id(IntPtr member);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_group_instance_id(IntPtr member);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberDescription_consumer_id(IntPtr member);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_consumer_id(IntPtr member);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberDescription_host(IntPtr member);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_host(IntPtr member);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberDescription_assignment(IntPtr member);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberDescription_assignment(IntPtr member);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_MemberAssignment_partitions(IntPtr assignment);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_MemberAssignment_partitions(IntPtr assignment);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_Node_id(IntPtr node);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_id(IntPtr node);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_Node_host(IntPtr node);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_host(IntPtr node);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_Node_port(IntPtr node);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_Node_port(IntPtr node);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_topic_result_error(IntPtr topicres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_topic_result_error(IntPtr topicres);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_topic_result_error_string(IntPtr topicres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_error_string(IntPtr topicres);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_topic_result_name(IntPtr topicres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_topic_result_name(IntPtr topicres);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_group_result_name(IntPtr groupres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_name(IntPtr groupres);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_group_result_error(IntPtr groupres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_error(IntPtr groupres);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_group_result_partitions(IntPtr groupres);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_group_result_partitions(IntPtr groupres);
#endif

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_DescribeUserScramCredentials(
                IntPtr handle,
                [MarshalAs(UnmanagedType.LPArray)] string[] users,
                UIntPtr usersCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_AlterUserScramCredentials(
                IntPtr handle,
                IntPtr[] alterations,
                UIntPtr alterationsCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_AlterUserScramCredentials(
                IntPtr handle,
                IntPtr[] alterations,
                UIntPtr alterationsCnt,
                IntPtr optionsPtr,
                IntPtr resultQueuePtr
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_UserScramCredentialDeletion_new(
                [MarshalAs(UnmanagedType.LPStr)] string user,
                ScramMechanism mechanism
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialDeletion_new(
                string user,
                ScramMechanism mechanism
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_UserScramCredentialUpsertion_new(
                [MarshalAs(UnmanagedType.LPStr)] string user,
                ScramMechanism mechanism,
                int iterations,
                byte[] password,
                IntPtr passwordSize,
                byte[] salt,
                IntPtr saltSize
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialUpsertion_new(
                string user,
                ScramMechanism mechanism,
                int iterations,
                byte[] password,
                IntPtr passwordSize,
                byte[] salt,
                IntPtr saltSize
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_UserScramCredentialAlteration_destroy(
                IntPtr alteration
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_UserScramCredentialAlteration_destroy(
                IntPtr alteration
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_DescribeUserScramCredentials_result_descriptions(
                IntPtr event_result,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_DescribeUserScramCredentials_result_descriptions(
                IntPtr event_result,
                out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_UserScramCredentialsDescription_user(IntPtr description);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_user(IntPtr description);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_UserScramCredentialsDescription_error(IntPtr description);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_error(IntPtr description);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(IntPtr description);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(IntPtr description);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(IntPtr description, int i);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(IntPtr description, int i);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ScramMechanism rd_kafka_ScramCredentialInfo_mechanism(IntPtr scramcredentialinfo);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ScramMechanism rd_kafka_ScramCredentialInfo_mechanism(IntPtr scramcredentialinfo);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial int rd_kafka_ScramCredentialInfo_iterations(IntPtr scramcredentialinfo);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern int rd_kafka_ScramCredentialInfo_iterations(IntPtr scramcredentialinfo);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AlterUserScramCredentials_result_responses(
                IntPtr event_result,
                out UIntPtr cntp
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_responses(
                IntPtr event_result,
                out UIntPtr cntp
                );
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AlterUserScramCredentials_result_response_user(IntPtr element);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_response_user(IntPtr element);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_AlterUserScramCredentials_result_response_error(IntPtr element);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_AlterUserScramCredentials_result_response_error(IntPtr element);
#endif

        //
        // Queues
        //

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_queue_new(IntPtr rk);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_queue_new(IntPtr rk);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_queue_destroy(IntPtr rkqu);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_queue_destroy(IntPtr rkqu);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_queue_poll(
                IntPtr rkqu,
                IntPtr timeout_ms
                );
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_queue_poll(
                IntPtr rkqu,
                IntPtr timeout_ms
                );
#endif

        //
        // Events
        //

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_event_destroy(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_event_destroy(IntPtr rkev);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial Librdkafka.EventType rd_kafka_event_type(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern Librdkafka.EventType rd_kafka_event_type(IntPtr rkev);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_event_opaque(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_opaque(IntPtr rkev);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_event_error(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_event_error(IntPtr rkev);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_event_error_string(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_error_string(IntPtr rkev);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_event_topic_partition_list(IntPtr rkev);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_event_topic_partition_list(IntPtr rkev);
#endif

        //
        // error_t
        //

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial ErrorCode rd_kafka_error_code(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern ErrorCode rd_kafka_error_code(IntPtr error);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_error_string(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_string(IntPtr error);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_error_is_fatal(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_is_fatal(IntPtr error);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_error_is_retriable(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_is_retriable(IntPtr error);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial IntPtr rd_kafka_error_txn_requires_abort(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr rd_kafka_error_txn_requires_abort(IntPtr error);
#endif

#if NET7_0_OR_GREATER
        [LibraryImport(DllName)]
#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        internal static partial void rd_kafka_error_destroy(IntPtr error);
#else
        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void rd_kafka_error_destroy(IntPtr error);
#endif
    }
}
