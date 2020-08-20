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
using System.IO;
using System.Text;
using System.Runtime.InteropServices;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Impl.NativeMethods;
using System.Reflection;
#if NET45 || NET46 || NET47
using System.ComponentModel;
#endif


namespace Confluent.Kafka.Impl
{
    internal static class Librdkafka
    {
        internal enum DestroyFlags
        {
            /*!
            * Don't call consumer_close() to leave group and commit final offsets.
            *
            * This also disables consumer callbacks to be called from rd_kafka_destroy*(),
            * such as rebalance_cb.
            *
            * The consumer group handler is still closed internally, but from an
            * application perspective none of the functionality from consumer_close()
            * is performed.
            */
            RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE = 0x8
        }

        internal enum AdminOp
        {
            Any = 0,
            CreateTopics = 1,
            DeleteTopics = 2,
            CreatePartitions=  3,
            AlterConfigs = 4,
            DescribeConfigs = 5
        }

        public enum EventType : int
        {
            None = 0x0,
            DR = 0x1,
            Fetch = 0x2,
            Log = 0x4,
            Error = 0x8,
            Rebalance = 0x10,
            Offset_Commit = 0x20,
            Stats = 0x40,
            CreateTopics_Result = 100,
            DeleteTopics_Result = 101,
            CreatePartitions_Result = 102,
            AlterConfigs_Result = 103,
            DescribeConfigs_Result = 104
        }

        // min librdkafka version, to change when binding to new function are added
        const long minVersion = 0x000903ff;

        // max length for error strings built by librdkafka
        internal const int MaxErrorStringLength = 512;

        private static class WindowsNative
        {
            [Flags]
            public enum LoadLibraryFlags : uint
            {
                DONT_RESOLVE_DLL_REFERENCES = 0x00000001,
                LOAD_IGNORE_CODE_AUTHZ_LEVEL = 0x00000010,
                LOAD_LIBRARY_AS_DATAFILE = 0x00000002,
                LOAD_LIBRARY_AS_DATAFILE_EXCLUSIVE = 0x00000040,
                LOAD_LIBRARY_AS_IMAGE_RESOURCE = 0x00000020,
                LOAD_LIBRARY_SEARCH_APPLICATION_DIR = 0x00000200,
                LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000,
                LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100,
                LOAD_LIBRARY_SEARCH_SYSTEM32 = 0x00000800,
                LOAD_LIBRARY_SEARCH_USER_DIRS = 0x00000400,
                LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008
            }

            [DllImport("kernel32", SetLastError = true)]
            public static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, LoadLibraryFlags dwFlags);

            [DllImport("kernel32", SetLastError = true)]
            public static extern IntPtr GetModuleHandle(string lpFileName);

            [DllImport("kernel32", SetLastError = true)]
            public static extern IntPtr GetProcAddress(IntPtr hModule, String procname);
        }

        private static class PosixNative
        {
            [DllImport("libdl")]
            public static extern IntPtr dlopen(String fileName, int flags);

            [DllImport("libdl")]
            public static extern IntPtr dlerror();

            [DllImport("libdl")]
            public static extern IntPtr dlsym(IntPtr handle, String symbol);

            public static string LastError
            {
                get
                {
                    // TODO: In practice, the following is always returning IntPtr.Zero. Why?
                    IntPtr error = dlerror();
                    if (error == IntPtr.Zero)
                    {
                        return "";
                    }
                    return Marshal.PtrToStringAnsi(error);
                }
            }
        }

        static bool SetDelegates(Type nativeMethodsClass)
        {
            var methods = nativeMethodsClass.GetRuntimeMethods().ToArray();

            _version = (Func<IntPtr>)methods.Single(m => m.Name == "rd_kafka_version").CreateDelegate(typeof(Func<IntPtr>));
            _version_str = (Func<IntPtr>)methods.Single(m => m.Name == "rd_kafka_version_str").CreateDelegate(typeof(Func<IntPtr>));
            _get_debug_contexts = (Func<IntPtr>)methods.Single(m => m.Name == "rd_kafka_get_debug_contexts").CreateDelegate(typeof(Func<IntPtr>));
            _err2str = (Func<ErrorCode, IntPtr>)methods.Single(m => m.Name == "rd_kafka_err2str").CreateDelegate(typeof(Func<ErrorCode, IntPtr>));
            _last_error = (Func<ErrorCode>)methods.Single(m => m.Name == "rd_kafka_last_error").CreateDelegate(typeof(Func<ErrorCode>));
            _fatal_error = (Func<IntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_fatal_error").CreateDelegate(typeof(Func<IntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _topic_partition_list_new = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_partition_list_new").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_partition_list_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_partition_list_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _topic_partition_list_add = (Func<IntPtr, string, int, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_partition_list_add").CreateDelegate(typeof(Func<IntPtr, string, int, IntPtr>));
            _headers_new = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_headers_new").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _headers_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_headers_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _header_add = (Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_header_add").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr, ErrorCode>));
            _header_get_all = (headerGetAllDelegate)methods.Single(m => m.Name == "rd_kafka_header_get_all").CreateDelegate(typeof(headerGetAllDelegate));
            _message_timestamp = (messageTimestampDelegate)methods.Single(m => m.Name == "rd_kafka_message_timestamp").CreateDelegate(typeof(messageTimestampDelegate));
            _message_headers = (messageHeadersDelegate)methods.Single(m => m.Name == "rd_kafka_message_headers").CreateDelegate(typeof(messageHeadersDelegate));
            _message_status = (Func<IntPtr, PersistenceStatus>)methods.Single(m => m.Name == "rd_kafka_message_status").CreateDelegate(typeof(Func<IntPtr, PersistenceStatus>));
            _message_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_message_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _conf_new = (Func<SafeConfigHandle>)methods.Single(m => m.Name == "rd_kafka_conf_new").CreateDelegate(typeof(Func<SafeConfigHandle>));
            _conf_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_conf_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _conf_dup = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_conf_dup").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _conf_set = (Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>)methods.Single(m => m.Name == "rd_kafka_conf_set").CreateDelegate(typeof(Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>));
            _conf_set_dr_msg_cb = (Action<IntPtr, DeliveryReportDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_dr_msg_cb").CreateDelegate(typeof(Action<IntPtr, DeliveryReportDelegate>));
            _conf_set_rebalance_cb = (Action<IntPtr, RebalanceDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_rebalance_cb").CreateDelegate(typeof(Action<IntPtr, RebalanceDelegate>));
            _conf_set_error_cb = (Action<IntPtr, ErrorDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_error_cb").CreateDelegate(typeof(Action<IntPtr, ErrorDelegate>));
            _conf_set_offset_commit_cb = (Action<IntPtr, CommitDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_offset_commit_cb").CreateDelegate(typeof(Action<IntPtr, CommitDelegate>));
            _conf_set_log_cb = (Action<IntPtr, LogDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_log_cb").CreateDelegate(typeof(Action<IntPtr, LogDelegate>));
            _conf_set_stats_cb = (Action<IntPtr, StatsDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_stats_cb").CreateDelegate(typeof(Action<IntPtr, StatsDelegate>));
            _conf_set_oauthbearer_token_refresh_cb = (Action<IntPtr, OAuthBearerTokenRefreshDelegate>)methods.Single(m => m.Name == "rd_kafka_conf_set_oauthbearer_token_refresh_cb").CreateDelegate(typeof(Action<IntPtr, OAuthBearerTokenRefreshDelegate>));
            _oauthbearer_set_token = (Func<IntPtr, string, long, string, string[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_oauthbearer_set_token").CreateDelegate(typeof(Func<IntPtr, string, long, string, string[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _oauthbearer_set_token_failure = (Func<IntPtr, string, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_oauthbearer_set_token_failure").CreateDelegate(typeof(Func<IntPtr, string, ErrorCode>));
            _conf_set_default_topic_conf = (Action<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_conf_set_default_topic_conf").CreateDelegate(typeof(Action<IntPtr, IntPtr>));
            _conf_get = (ConfGet)methods.Single(m => m.Name == "rd_kafka_conf_get").CreateDelegate(typeof(ConfGet));
            _topic_conf_get = (ConfGet)methods.Single(m => m.Name == "rd_kafka_topic_conf_get").CreateDelegate(typeof(ConfGet));
            _conf_dump = (ConfDump)methods.Single(m => m.Name == "rd_kafka_conf_dump").CreateDelegate(typeof(ConfDump));
            _topic_conf_dump = (ConfDump)methods.Single(m => m.Name == "rd_kafka_topic_conf_dump").CreateDelegate(typeof(ConfDump));
            _conf_dump_free = (Action<IntPtr, UIntPtr>)methods.Single(m => m.Name == "rd_kafka_conf_dump_free").CreateDelegate(typeof(Action<IntPtr, UIntPtr>));
            _topic_conf_new = (Func<SafeTopicConfigHandle>)methods.Single(m => m.Name == "rd_kafka_topic_conf_new").CreateDelegate(typeof(Func<SafeTopicConfigHandle>));
            _topic_conf_dup = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_conf_dup").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_conf_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_conf_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _topic_conf_set = (Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>)methods.Single(m => m.Name == "rd_kafka_topic_conf_set").CreateDelegate(typeof(Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>));
            _topic_conf_set_partitioner_cb = (Action<IntPtr, PartitionerDelegate>)methods.Single(m => m.Name == "rd_kafka_topic_conf_set_partitioner_cb").CreateDelegate(typeof(Action<IntPtr, PartitionerDelegate>));
            _topic_partition_available = (Func<IntPtr, int, bool>)methods.Single(m => m.Name == "rd_kafka_topic_partition_available").CreateDelegate(typeof(Func<IntPtr, int, bool>));
            _init_transactions = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_init_transactions").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _begin_transaction = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_begin_transaction").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _commit_transaction = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_commit_transaction").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _abort_transaction = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_abort_transaction").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _send_offsets_to_transaction = (Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_send_offsets_to_transaction").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr>));
            _rd_kafka_consumer_group_metadata = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_consumer_group_metadata").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _rd_kafka_consumer_group_metadata_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_consumer_group_metadata_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _rd_kafka_consumer_group_metadata_write = (ConsumerGroupMetadataWriteDelegate)methods.Single(m => m.Name == "rd_kafka_consumer_group_metadata_write").CreateDelegate(typeof(ConsumerGroupMetadataWriteDelegate));
            _rd_kafka_consumer_group_metadata_read = (ConsumerGroupMetadataReadDelegate)methods.Single(m => m.Name == "rd_kafka_consumer_group_metadata_read").CreateDelegate(typeof(ConsumerGroupMetadataReadDelegate));
            _new = (Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle>)methods.Single(m => m.Name == "rd_kafka_new").CreateDelegate(typeof(Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle>));
            _name = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_name").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _memberid = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_memberid").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_new = (Func<IntPtr, string, IntPtr, SafeTopicHandle>)methods.Single(m => m.Name == "rd_kafka_topic_new").CreateDelegate(typeof(Func<IntPtr, string, IntPtr, SafeTopicHandle>));
            _topic_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _topic_name = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_name").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_poll").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _poll_set_consumer = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_poll_set_consumer").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _query_watermark_offsets = (QueryOffsets)methods.Single(m => m.Name == "rd_kafka_query_watermark_offsets").CreateDelegate(typeof(QueryOffsets));
            _get_watermark_offsets = (GetOffsets)methods.Single(m => m.Name == "rd_kafka_get_watermark_offsets").CreateDelegate(typeof(GetOffsets));
            _offsets_for_times = (OffsetsForTimes)methods.Single(m => m.Name == "rd_kafka_offsets_for_times").CreateDelegate(typeof(OffsetsForTimes));
            _mem_free = (Action<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_mem_free").CreateDelegate(typeof(Action<IntPtr, IntPtr>));
            _subscribe = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_subscribe").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _unsubscribe = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_unsubscribe").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _subscription = (Subscription)methods.Single(m => m.Name == "rd_kafka_subscription").CreateDelegate(typeof(Subscription));
            _consumer_poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_consumer_poll").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _consumer_close = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_consumer_close").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _assign = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_assign").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _assignment = (Assignment)methods.Single(m => m.Name == "rd_kafka_assignment").CreateDelegate(typeof(Assignment));
            _offsets_store = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_offsets_store").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _commit = (Func<IntPtr, IntPtr, bool, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_commit").CreateDelegate(typeof(Func<IntPtr, IntPtr, bool, ErrorCode>));
            _commit_queue = (Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_commit_queue").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode>));
            _committed = (Func<IntPtr, IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_committed").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, ErrorCode>));
            _pause_partitions = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_pause_partitions").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _resume_partitions = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_resume_partitions").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _seek = (Func<IntPtr, int, long, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_seek").CreateDelegate(typeof(Func<IntPtr, int, long, IntPtr, ErrorCode>));
            _position = (Func<IntPtr, IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_position").CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _producev = (Producev)methods.Single(m => m.Name == "rd_kafka_producev").CreateDelegate(typeof(Producev));
            _flush = (Flush)methods.Single(m => m.Name == "rd_kafka_flush").CreateDelegate(typeof(Flush));
            _metadata = (Metadata)methods.Single(m => m.Name == "rd_kafka_metadata").CreateDelegate(typeof(Metadata));
            _metadata_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_metadata_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _list_groups = (ListGroups)methods.Single(m => m.Name == "rd_kafka_list_groups").CreateDelegate(typeof(ListGroups));
            _group_list_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_group_list_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _brokers_add = (Func<IntPtr, string, IntPtr>)methods.Single(m => m.Name == "rd_kafka_brokers_add").CreateDelegate(typeof(Func<IntPtr, string, IntPtr>));
            _outq_len = (Func<IntPtr, int>)methods.Single(m => m.Name == "rd_kafka_outq_len").CreateDelegate(typeof(Func<IntPtr, int>));
            _queue_new = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_queue_new").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _queue_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_queue_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _event_opaque = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_event_opaque").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _event_type = (Func<IntPtr, EventType>)methods.Single(m => m.Name == "rd_kafka_event_type").CreateDelegate(typeof(Func<IntPtr, EventType>));
            _event_error = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_event_error").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _event_error_string = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_event_error_string").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _event_topic_partition_list = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_event_topic_partition_list").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _event_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_event_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _queue_poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_queue_poll").CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            
            _AdminOptions_new = (Func<IntPtr, AdminOp, IntPtr>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_new").CreateDelegate(typeof(Func<IntPtr, AdminOp, IntPtr>));
            _AdminOptions_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _AdminOptions_set_request_timeout = (Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_request_timeout").CreateDelegate(typeof(Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _AdminOptions_set_operation_timeout = (Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_operation_timeout").CreateDelegate(typeof(Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _AdminOptions_set_validate_only = (Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_validate_only").CreateDelegate(typeof(Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _AdminOptions_set_incremental = (Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_incremental").CreateDelegate(typeof(Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _AdminOptions_set_broker = (Func<IntPtr, int, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_broker").CreateDelegate(typeof(Func<IntPtr, int, StringBuilder, UIntPtr, ErrorCode>));
            _AdminOptions_set_opaque = (Action<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_AdminOptions_set_opaque").CreateDelegate(typeof(Action<IntPtr, IntPtr>));

            _NewTopic_new = (Func<string, IntPtr, IntPtr, StringBuilder, UIntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_NewTopic_new").CreateDelegate(typeof(Func<string, IntPtr, IntPtr, StringBuilder, UIntPtr, IntPtr>));
            _NewTopic_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_NewTopic_destroy").CreateDelegate(typeof(Action<IntPtr>));

            _NewTopic_set_replica_assignment = (Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_NewTopic_set_replica_assignment").CreateDelegate(typeof(Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>));
            _NewTopic_set_config = (Func<IntPtr, string, string, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_NewTopic_set_config").CreateDelegate(typeof(Func<IntPtr, string, string, ErrorCode>));

            _CreateTopics = (Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_CreateTopics").CreateDelegate(typeof(Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>));
            _CreateTopics_result_topics = (_CreateTopics_result_topics_delegate)methods.Single(m => m.Name == "rd_kafka_CreateTopics_result_topics").CreateDelegate(typeof(_CreateTopics_result_topics_delegate));

            _DeleteTopic_new = (Func<string, IntPtr>)methods.Single(m => m.Name == "rd_kafka_DeleteTopic_new").CreateDelegate(typeof(Func<string, IntPtr>));
            _DeleteTopic_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_DeleteTopic_destroy").CreateDelegate(typeof(Action<IntPtr>));

            _DeleteTopics = (Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_DeleteTopics").CreateDelegate(typeof(Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>));
            _DeleteTopics_result_topics = (_DeleteTopics_result_topics_delegate)methods.Single(m => m.Name == "rd_kafka_DeleteTopics_result_topics").CreateDelegate(typeof(_DeleteTopics_result_topics_delegate));

            _NewPartitions_new = (Func<string, UIntPtr, StringBuilder, UIntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_NewPartitions_new").CreateDelegate(typeof(Func<string, UIntPtr, StringBuilder, UIntPtr, IntPtr>));
            _NewPartitions_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_NewPartitions_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _NewPartitions_set_replica_assignment = (Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_NewPartitions_set_replica_assignment").CreateDelegate(typeof(Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode>));

            _CreatePartitions = (Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_CreatePartitions").CreateDelegate(typeof(Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>));
            _CreatePartitions_result_topics = (_CreatePartitions_result_topics_delegate)methods.Single(m => m.Name == "rd_kafka_CreatePartitions_result_topics").CreateDelegate(typeof(_CreatePartitions_result_topics_delegate));

            _ConfigSource_name = (Func<ConfigSource, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigSource_name").CreateDelegate(typeof(Func<ConfigSource, IntPtr>));
            _ConfigEntry_name = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_name").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_value = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_value").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_source = (Func<IntPtr, ConfigSource>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_source").CreateDelegate(typeof(Func<IntPtr, ConfigSource>));
            _ConfigEntry_is_read_only = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_is_read_only").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_is_default = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_is_default").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_is_sensitive = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_is_sensitive").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_is_synonym = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_is_synonym").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigEntry_synonyms = (_ConfigEntry_synonyms_delegate)methods.Single(m => m.Name == "rd_kafka_ConfigEntry_synonyms").CreateDelegate(typeof(_ConfigEntry_synonyms_delegate));

            _ResourceType_name = (Func<ResourceType, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ResourceType_name").CreateDelegate(typeof(Func<ResourceType, IntPtr>));

            _ConfigResource_new = (Func<ResourceType, string, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_new").CreateDelegate(typeof(Func<ResourceType, string, IntPtr>));
            _ConfigResource_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _ConfigResource_add_config = (Func<IntPtr, string, string, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_add_config").CreateDelegate(typeof(Func<IntPtr, string, string, ErrorCode>));
            _ConfigResource_set_config = (Func<IntPtr, string, string, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_set_config").CreateDelegate(typeof(Func<IntPtr, string, string, ErrorCode>));
            _ConfigResource_delete_config = (Func<IntPtr, string, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_delete_config").CreateDelegate(typeof(Func<IntPtr, string, ErrorCode>));
            _ConfigResource_configs = (_ConfigResource_configs_delegate)methods.Single(m => m.Name == "rd_kafka_ConfigResource_configs").CreateDelegate(typeof(_ConfigResource_configs_delegate));

            _ConfigResource_type = (Func<IntPtr, ResourceType>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_type").CreateDelegate(typeof(Func<IntPtr, ResourceType>));
            _ConfigResource_name = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_name").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _ConfigResource_error = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_error").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _ConfigResource_error_string = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_ConfigResource_error_string").CreateDelegate(typeof(Func<IntPtr, IntPtr>));

            _AlterConfigs = (Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_AlterConfigs").CreateDelegate(typeof(Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>));
            _AlterConfigs_result_resources = (_AlterConfigs_result_resources_delegate)methods.Single(m => m.Name == "rd_kafka_AlterConfigs_result_resources").CreateDelegate(typeof(_AlterConfigs_result_resources_delegate));

            _DescribeConfigs = (Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_DescribeConfigs").CreateDelegate(typeof(Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr>));
            _DescribeConfigs_result_resources = (_DescribeConfigs_result_resources_delegate)methods.Single(m => m.Name == "rd_kafka_DescribeConfigs_result_resources").CreateDelegate(typeof(_DescribeConfigs_result_resources_delegate));

            _topic_result_error = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_topic_result_error").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _topic_result_error_string = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_result_error_string").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_result_name = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_topic_result_name").CreateDelegate(typeof(Func<IntPtr, IntPtr>));

            _destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_destroy").CreateDelegate(typeof(Action<IntPtr>));
            _destroy_flags = (Action<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_destroy_flags").CreateDelegate(typeof(Action<IntPtr, IntPtr>));

            _error_code = (Func<IntPtr, ErrorCode>)methods.Single(m => m.Name == "rd_kafka_error_code").CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _error_string = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_error_string").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _error_is_fatal = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_error_is_fatal").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _error_is_retriable = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_error_is_retriable").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _error_txn_requires_abort = (Func<IntPtr, IntPtr>)methods.Single(m => m.Name == "rd_kafka_error_txn_requires_abort").CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _error_destroy = (Action<IntPtr>)methods.Single(m => m.Name == "rd_kafka_error_destroy").CreateDelegate(typeof(Action<IntPtr>));

            try
            {
                // throws if the native library failed to load.
                _err2str(ErrorCode.NoError);
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        static object loadLockObj = new object();
        static bool isInitialized = false;

        public static bool IsInitialized
        {
            get
            {
                lock (loadLockObj)
                {
                    return isInitialized;
                }
            }
        }

        public static bool Initialize(string userSpecifiedPath)
        {
            lock (loadLockObj)
            {
                if (isInitialized)
                {
                    return false;
                }

                isInitialized = false;

#if NET45 || NET46 || NET47

                string path = userSpecifiedPath;
                if (path == null)
                {
                    // in net45, librdkafka.dll is not in the process directory, we have to load it manually
                    // and also search in the same folder for its dependencies (LOAD_WITH_ALTERED_SEARCH_PATH)
                    var is64 = IntPtr.Size == 8;
                    var baseUri = new Uri(Assembly.GetExecutingAssembly().GetName().EscapedCodeBase);
                    var baseDirectory = Path.GetDirectoryName(baseUri.LocalPath);
                    var dllDirectory = Path.Combine(
                        baseDirectory,
                        is64
                            ? Path.Combine("librdkafka", "x64")
                            : Path.Combine("librdkafka", "x86"));
                    path = Path.Combine(dllDirectory, "librdkafka.dll");

                    if (!File.Exists(path))
                    {
                        dllDirectory = Path.Combine(
                            baseDirectory,
                            is64
                                ? @"runtimes\win-x64\native"
                                : @"runtimes\win-x86\native");
                        path = Path.Combine(dllDirectory, "librdkafka.dll");
                    }

                    if (!File.Exists(path))
                    {
                        dllDirectory = Path.Combine(
                            baseDirectory,
                            is64 ? "x64" : "x86");
                        path = Path.Combine(dllDirectory, "librdkafka.dll");
                    }

                    if (!File.Exists(path))
                    {

                        path = Path.Combine(baseDirectory, "librdkafka.dll");
                    }
                }

                if (WindowsNative.LoadLibraryEx(path, IntPtr.Zero, WindowsNative.LoadLibraryFlags.LOAD_WITH_ALTERED_SEARCH_PATH) == IntPtr.Zero)
                {
                    // catch the last win32 error by default and keep the associated default message
                    var win32Exception = new Win32Exception();
                    var additionalMessage =
                        $"Error while loading librdkafka.dll or its dependencies from {path}. " +
                        $"Check the directory exists, if not check your deployment process. " +
                        $"You can also load the library and its dependencies by yourself " +
                        $"before any call to Confluent.Kafka";

                    throw new InvalidOperationException(additionalMessage, win32Exception);
                }

                isInitialized = SetDelegates(typeof(NativeMethods.NativeMethods));

#else

                const int RTLD_NOW = 2;

                var nativeMethodTypes = new List<Type>();

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    if (userSpecifiedPath != null)
                    {
                        if (WindowsNative.LoadLibraryEx(userSpecifiedPath, IntPtr.Zero, WindowsNative.LoadLibraryFlags.LOAD_WITH_ALTERED_SEARCH_PATH) == IntPtr.Zero)
                        {
                            // TODO: The Win32Exception class is not available in .NET Standard, which is the easy way to get the message string corresponding to
                            // a win32 error. FormatMessage is not straightforward to p/invoke, so leaving this as a job for another day.
                            throw new InvalidOperationException($"Failed to load librdkafka at location '{userSpecifiedPath}'. Win32 error: {Marshal.GetLastWin32Error()}");
                        }
                    }

                    nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods));
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    if (userSpecifiedPath != null)
                    {
                        if (PosixNative.dlopen(userSpecifiedPath, RTLD_NOW) == IntPtr.Zero)
                        {
                            throw new InvalidOperationException($"Failed to load librdkafka at location '{userSpecifiedPath}'. dlerror: '{PosixNative.LastError}'.");
                        }
                    }

                    nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods));
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    if (userSpecifiedPath != null)
                    {
                        if (PosixNative.dlopen(userSpecifiedPath, RTLD_NOW) == IntPtr.Zero)
                        {
                            throw new InvalidOperationException($"Failed to load librdkafka at location '{userSpecifiedPath}'. dlerror: '{PosixNative.LastError}'.");
                        }

                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods));
                    }
                    else
                    {
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods_Centos7));
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods));
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods_Debian9));
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods_Alpine));
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Unsupported platform: {RuntimeInformation.OSDescription}");
                }

                foreach (var t in nativeMethodTypes)
                {
                    isInitialized = SetDelegates(t);
                    if (isInitialized)
                    {
                        break;
                    }
                }

#endif

                if (!isInitialized)
                {
                    throw new DllNotFoundException("Failed to load the librdkafka native library.");
                }

                if ((long)version() < minVersion)
                {
                    throw new FileLoadException($"Invalid librdkafka version {(long)version():x}, expected at least {minVersion:x}");
                }

                isInitialized = true;


                return isInitialized;
            }
        }

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void DeliveryReportDelegate(
                IntPtr rk,
                /* const rd_kafka_message_t * */ IntPtr rkmessage,
                // ref rd_kafka_message rkmessage,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void CommitDelegate(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void ErrorDelegate(IntPtr rk,
                ErrorCode err, string reason, IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void RebalanceDelegate(IntPtr rk,
                ErrorCode err,
                /* rd_kafka_topic_partition_list_t * */ IntPtr partitions,
                IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void LogDelegate(IntPtr rk, SyslogLevel level, string fac, string buf);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate int StatsDelegate(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate void OAuthBearerTokenRefreshDelegate(IntPtr rk, IntPtr oauthbearer_config, IntPtr opaque);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate int PartitionerDelegate(
            /* const rd_kafka_topic_t * */ IntPtr rkt,
            IntPtr keydata,
            UIntPtr keylen,
            int partition_cnt,
            IntPtr rkt_opaque,
            IntPtr msg_opaque);


        private static Func<IntPtr> _version;
        internal static IntPtr version() => _version();

        private static Func<IntPtr> _version_str;
        internal static IntPtr version_str() => _version_str();

        private static Func<IntPtr> _get_debug_contexts;
        internal static IntPtr get_debug_contexts() => _get_debug_contexts();

        private static Func<ErrorCode, IntPtr> _err2str;
        internal static IntPtr err2str(ErrorCode err) => _err2str(err);

        private static Func<IntPtr, IntPtr> _topic_partition_list_new;
        internal static IntPtr topic_partition_list_new(IntPtr size)
            => _topic_partition_list_new(size);

        private static Action<IntPtr> _topic_partition_list_destroy;
        internal static void topic_partition_list_destroy(IntPtr rkparlist)
            => _topic_partition_list_destroy(rkparlist);

        private static Func<IntPtr, string, int, IntPtr> _topic_partition_list_add;
        internal static IntPtr topic_partition_list_add(IntPtr rktparlist,
                string topic, int partition)
            => _topic_partition_list_add(rktparlist, topic, partition);

        private static Func<IntPtr, IntPtr> _headers_new;
        internal static IntPtr headers_new(IntPtr size)
            => _headers_new(size);

        private static Action<IntPtr> _headers_destroy;
        internal static void headers_destroy(IntPtr hdrs)
            => _headers_destroy(hdrs);

        private static Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr, ErrorCode> _header_add;
        internal static ErrorCode headers_add(
                IntPtr hdrs,
                IntPtr keydata,
                IntPtr keylen,
                IntPtr valdata,
                IntPtr vallen)
            => _header_add(hdrs, keydata, keylen, valdata, vallen);

        internal delegate ErrorCode headerGetAllDelegate(
            IntPtr hdrs, 
            IntPtr idx,
            out IntPtr namep,
            out IntPtr valuep,
            out IntPtr sizep);
        private static headerGetAllDelegate _header_get_all;
        internal static ErrorCode header_get_all(
            /* const rd_kafka_headers_t * */ IntPtr hdrs,
            /* const size_t */ IntPtr idx,
            /* const char ** */ out IntPtr namep,
            /* const void ** */ out IntPtr valuep,
            /* size_t * */ out IntPtr sizep)
            => _header_get_all(hdrs, idx, out namep, out valuep, out sizep);

        private static Func<ErrorCode> _last_error;
        internal static ErrorCode last_error() => _last_error();

        private static Func<IntPtr, StringBuilder, UIntPtr, ErrorCode> _fatal_error;
        internal static ErrorCode fatal_error(IntPtr rk, StringBuilder sb, UIntPtr len) => _fatal_error(rk, sb, len);

        internal delegate long messageTimestampDelegate(IntPtr rkmessage, out IntPtr tstype);
        private static messageTimestampDelegate _message_timestamp;
        internal static long message_timestamp(IntPtr rkmessage, out IntPtr tstype) => _message_timestamp(rkmessage, out tstype);

        private static Func<IntPtr, PersistenceStatus> _message_status;
        internal static PersistenceStatus message_status(IntPtr rkmessage) => _message_status(rkmessage);

        internal delegate ErrorCode messageHeadersDelegate(IntPtr rkmessage, out IntPtr hdrsType);
        private static messageHeadersDelegate _message_headers;
        internal static ErrorCode message_headers(IntPtr rkmessage, out IntPtr hdrs) => _message_headers(rkmessage, out hdrs);

        private static Action<IntPtr> _message_destroy;
        internal static void message_destroy(IntPtr rkmessage) => _message_destroy(rkmessage);

        private static Func<SafeConfigHandle> _conf_new;
        internal static SafeConfigHandle conf_new() => _conf_new();

        private static Action<IntPtr> _conf_destroy;
        internal static void conf_destroy(IntPtr conf) => _conf_destroy(conf);

        private static Func<IntPtr, IntPtr> _conf_dup;
        internal static IntPtr conf_dup(IntPtr conf) => _conf_dup(conf);

        private static Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes> _conf_set;
        internal static ConfRes conf_set(IntPtr conf, string name,
                string value, StringBuilder errstr, UIntPtr errstr_size)
            => _conf_set(conf, name, value, errstr, errstr_size);

        private static Action<IntPtr, DeliveryReportDelegate> _conf_set_dr_msg_cb;
        internal static void conf_set_dr_msg_cb(IntPtr conf, DeliveryReportDelegate dr_msg_cb)
            => _conf_set_dr_msg_cb(conf, dr_msg_cb);

        private static Action<IntPtr, RebalanceDelegate> _conf_set_rebalance_cb;
        internal static void conf_set_rebalance_cb(IntPtr conf, RebalanceDelegate rebalance_cb)
            => _conf_set_rebalance_cb(conf, rebalance_cb);

        private static Action<IntPtr, CommitDelegate> _conf_set_offset_commit_cb;
        internal static void conf_set_offset_commit_cb(IntPtr conf, CommitDelegate commit_cb)
            => _conf_set_offset_commit_cb(conf, commit_cb);

        private static Action<IntPtr, ErrorDelegate> _conf_set_error_cb;
        internal static void conf_set_error_cb(IntPtr conf, ErrorDelegate error_cb)
            => _conf_set_error_cb(conf, error_cb);

        private static Action<IntPtr, LogDelegate> _conf_set_log_cb;
        internal static void conf_set_log_cb(IntPtr conf, LogDelegate log_cb)
            => _conf_set_log_cb(conf, log_cb);

        private static Action<IntPtr, StatsDelegate> _conf_set_stats_cb;
        internal static void conf_set_stats_cb(IntPtr conf, StatsDelegate stats_cb)
            => _conf_set_stats_cb(conf, stats_cb);

        private static Action<IntPtr, OAuthBearerTokenRefreshDelegate> _conf_set_oauthbearer_token_refresh_cb;
        internal static void conf_set_oauthbearer_token_refresh_cb(IntPtr conf, OAuthBearerTokenRefreshDelegate oauthbearer_token_refresh_cb)
            => _conf_set_oauthbearer_token_refresh_cb(conf, oauthbearer_token_refresh_cb);

        private static Func<IntPtr, string, long, string, string[], UIntPtr, StringBuilder, UIntPtr, ErrorCode> _oauthbearer_set_token;
        internal static ErrorCode oauthbearer_set_token(IntPtr rk,
            string token_value, long md_lifetime_ms, string md_principal_name, string[] extensions, UIntPtr extensions_size,
            StringBuilder errstr, UIntPtr errstr_size)
            => _oauthbearer_set_token(rk, token_value, md_lifetime_ms, md_principal_name, extensions, extensions_size, errstr, errstr_size);

        private static Func<IntPtr, string, ErrorCode> _oauthbearer_set_token_failure;
        internal static ErrorCode oauthbearer_set_token_failure(IntPtr rk, string errstr)
            => _oauthbearer_set_token_failure(rk, errstr);

        private static Action<IntPtr, IntPtr> _conf_set_default_topic_conf;
        internal static void conf_set_default_topic_conf(IntPtr conf, IntPtr tconf)
            => _conf_set_default_topic_conf(conf, tconf);

        private delegate ConfRes ConfGet(IntPtr conf, string name, StringBuilder dest,
                ref UIntPtr dest_size);
        private static ConfGet _conf_get;
        internal static ConfRes conf_get(IntPtr conf, string name,
                StringBuilder dest, ref UIntPtr dest_size)
            => _conf_get(conf, name, dest, ref dest_size);

        private static ConfGet _topic_conf_get;
        internal static ConfRes topic_conf_get(IntPtr conf, string name,
                StringBuilder dest, ref UIntPtr dest_size)
            => _topic_conf_get(conf, name, dest, ref dest_size);

        private delegate IntPtr ConfDump(IntPtr conf, out UIntPtr cntp);
        private static ConfDump _conf_dump;
        internal static IntPtr conf_dump(IntPtr conf, out UIntPtr cntp)
            => _conf_dump(conf, out cntp);

        private static ConfDump _topic_conf_dump;
        internal static IntPtr topic_conf_dump(IntPtr conf, out UIntPtr cntp)
            => _topic_conf_dump(conf, out cntp);

        private static Action<IntPtr, UIntPtr> _conf_dump_free;
        internal static void conf_dump_free(IntPtr arr, UIntPtr cnt)
            => _conf_dump_free(arr, cnt);

        private static Func<SafeTopicConfigHandle> _topic_conf_new;
        internal static SafeTopicConfigHandle topic_conf_new() => _topic_conf_new();

        private static Func<IntPtr, IntPtr> _topic_conf_dup;
        internal static IntPtr topic_conf_dup(IntPtr conf) => _topic_conf_dup(conf);

        private static Action<IntPtr> _topic_conf_destroy;
        internal static void topic_conf_destroy(IntPtr conf) => _topic_conf_destroy(conf);

        private static Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes> _topic_conf_set;
        internal static ConfRes topic_conf_set(IntPtr conf, string name,
                string value, StringBuilder errstr, UIntPtr errstr_size)
            => _topic_conf_set(conf, name, value, errstr, errstr_size);

        private static Action<IntPtr, PartitionerDelegate> _topic_conf_set_partitioner_cb;
        internal static void topic_conf_set_partitioner_cb(
                IntPtr topic_conf, PartitionerDelegate partitioner_cb)
            => _topic_conf_set_partitioner_cb(topic_conf, partitioner_cb);

        private static Func<IntPtr, int, bool> _topic_partition_available;
        internal static bool topic_partition_available(IntPtr rkt, int partition)
            => _topic_partition_available(rkt, partition);

        private static Func<IntPtr, IntPtr, IntPtr> _init_transactions;
        internal static IntPtr init_transactions(IntPtr rk, IntPtr timeout)
            => _init_transactions(rk, timeout);

        private static Func<IntPtr, IntPtr> _begin_transaction;
        internal static IntPtr begin_transaction(IntPtr rk)
            => _begin_transaction(rk);

        private static Func<IntPtr, IntPtr, IntPtr> _commit_transaction;
        internal static IntPtr commit_transaction(IntPtr rk, IntPtr timeout)
            => _commit_transaction(rk, timeout);

        private static Func<IntPtr, IntPtr, IntPtr> _abort_transaction;
        internal static IntPtr abort_transaction(IntPtr rk, IntPtr timeout)
            => _abort_transaction(rk, timeout);

        private static Func<IntPtr, IntPtr, IntPtr, IntPtr, IntPtr> _send_offsets_to_transaction;
        internal static IntPtr send_offsets_to_transaction(IntPtr rk, IntPtr offsets, IntPtr consumer_group_metadata, IntPtr timeout_ms)
            => _send_offsets_to_transaction(rk, offsets, consumer_group_metadata, timeout_ms);

        private static Func<IntPtr, IntPtr> _rd_kafka_consumer_group_metadata;
        internal static IntPtr consumer_group_metadata(IntPtr rk)
            => _rd_kafka_consumer_group_metadata(rk);

        private static Action<IntPtr> _rd_kafka_consumer_group_metadata_destroy;
        internal static void consumer_group_metadata_destroy(IntPtr rk)
            => _rd_kafka_consumer_group_metadata_destroy(rk);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        private delegate IntPtr ConsumerGroupMetadataWriteDelegate(IntPtr cgmd, out IntPtr data, out IntPtr dataSize);
        private static ConsumerGroupMetadataWriteDelegate _rd_kafka_consumer_group_metadata_write;
        internal static IntPtr consumer_group_metadata_write(IntPtr cgmd, out IntPtr data, out IntPtr dataSize)
            => _rd_kafka_consumer_group_metadata_write(cgmd, out data, out dataSize);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        private delegate IntPtr ConsumerGroupMetadataReadDelegate(out IntPtr cgmd, byte[] data, IntPtr dataSize);
        private static ConsumerGroupMetadataReadDelegate _rd_kafka_consumer_group_metadata_read;
        internal static IntPtr consumer_group_metadata_read(out IntPtr cgmd, byte[] data, IntPtr dataSize)
            => _rd_kafka_consumer_group_metadata_read(out cgmd, data, dataSize);

        private static Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle> _new;
        internal static SafeKafkaHandle kafka_new(RdKafkaType type, IntPtr conf,
                StringBuilder errstr, UIntPtr errstr_size)
            => _new(type, conf, errstr, errstr_size);

        private static Action<IntPtr> _destroy;
        internal static void destroy(IntPtr rk) => _destroy(rk);

        private static Action<IntPtr, IntPtr> _destroy_flags;
        internal static void destroy_flags(IntPtr rk, IntPtr flags) => _destroy_flags(rk, flags);

        private static Func<IntPtr, IntPtr> _name;
        internal static IntPtr name(IntPtr rk) => _name(rk);

        private static Func<IntPtr, IntPtr> _memberid;
        internal static IntPtr memberid(IntPtr rk) => _memberid(rk);

        private static Func<IntPtr, string, IntPtr, SafeTopicHandle> _topic_new;
        internal static SafeTopicHandle topic_new(IntPtr rk, string topic, IntPtr conf)
            => _topic_new(rk, topic, conf);

        private static Action<IntPtr> _topic_destroy;
        internal static void topic_destroy(IntPtr rk) => _topic_destroy(rk);

        private static Func<IntPtr, IntPtr> _topic_name;
        internal static IntPtr topic_name(IntPtr rkt) => _topic_name(rkt);

        private static Func<IntPtr, ErrorCode> _poll_set_consumer;
        internal static ErrorCode poll_set_consumer(IntPtr rk) => _poll_set_consumer(rk);

        private static Func<IntPtr, IntPtr, IntPtr> _poll;
        internal static IntPtr poll(IntPtr rk, IntPtr timeout_ms) => _poll(rk, timeout_ms);

        private delegate ErrorCode QueryOffsets(IntPtr rk, string topic, int partition,
                out long low, out long high, IntPtr timeout_ms);
        private static QueryOffsets _query_watermark_offsets;
        internal static ErrorCode query_watermark_offsets(IntPtr rk, string topic, int partition,
                out long low, out long high, IntPtr timeout_ms)
            => _query_watermark_offsets(rk, topic, partition, out low, out high, timeout_ms);

        private delegate ErrorCode GetOffsets(IntPtr rk, string topic, int partition,
                out long low, out long high);
        private static GetOffsets _get_watermark_offsets;
        internal static ErrorCode get_watermark_offsets(IntPtr rk, string topic, int partition,
                out long low, out long high)
            => _get_watermark_offsets(rk, topic, partition, out low, out high);

        private delegate ErrorCode OffsetsForTimes(IntPtr rk, IntPtr offsets, IntPtr timeout_ms);
        private static OffsetsForTimes _offsets_for_times;
        internal static ErrorCode offsets_for_times(IntPtr rk, IntPtr offsets, IntPtr timeout_ms)
            => _offsets_for_times(rk, offsets, timeout_ms);

        private static Action<IntPtr, IntPtr> _mem_free;
        internal static void mem_free(IntPtr rk, IntPtr ptr)
            => _mem_free(rk, ptr);

        private static Func<IntPtr, IntPtr, ErrorCode> _subscribe;
        internal static ErrorCode subscribe(IntPtr rk, IntPtr topics) => _subscribe(rk, topics);

        private static Func<IntPtr, ErrorCode> _unsubscribe;
        internal static ErrorCode unsubscribe(IntPtr rk) => _unsubscribe(rk);

        private delegate ErrorCode Subscription(IntPtr rk, out IntPtr topics);
        private static Subscription _subscription;
        internal static ErrorCode subscription(IntPtr rk, out IntPtr topics)
            => _subscription(rk, out topics);

        private static Func<IntPtr, IntPtr, IntPtr> _consumer_poll;
        internal static IntPtr consumer_poll(IntPtr rk, IntPtr timeout_ms)
            => _consumer_poll(rk, timeout_ms);

        private static Func<IntPtr, ErrorCode> _consumer_close;
        internal static ErrorCode consumer_close(IntPtr rk) => _consumer_close(rk);

        private static Func<IntPtr, IntPtr, ErrorCode> _assign;
        internal static ErrorCode assign(IntPtr rk, IntPtr partitions)
            => _assign(rk, partitions);

        private delegate ErrorCode Assignment(IntPtr rk, out IntPtr topics);
        private static Assignment _assignment;
        internal static ErrorCode assignment(IntPtr rk, out IntPtr topics)
            => _assignment(rk, out topics);

        private static Func<IntPtr, IntPtr, ErrorCode> _offsets_store;
        internal static ErrorCode offsets_store(IntPtr rk, IntPtr offsets)
            => _offsets_store(rk, offsets);

        private static Func<IntPtr, IntPtr, bool, ErrorCode> _commit;
        internal static ErrorCode commit(IntPtr rk, IntPtr offsets, bool async)
            => _commit(rk, offsets, async);

        private static Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode> _commit_queue;
        internal static ErrorCode commit_queue(IntPtr rk, IntPtr offsets, IntPtr rkqu,
            CommitDelegate cb, IntPtr opaque)
            => _commit_queue(rk, offsets, rkqu, cb, opaque);

        private static Func<IntPtr, IntPtr, ErrorCode> _pause_partitions;
        internal static ErrorCode pause_partitions(IntPtr rk, IntPtr partitions)
            => _pause_partitions(rk, partitions);

        private static Func<IntPtr, IntPtr, ErrorCode> _resume_partitions;
        internal static ErrorCode resume_partitions(IntPtr rk, IntPtr partitions)
            => _resume_partitions(rk, partitions);

        private static Func<IntPtr, int, long, IntPtr, ErrorCode> _seek;
        internal static ErrorCode seek(IntPtr rkt, int partition, long offset, IntPtr timeout_ms)
            => _seek(rkt, partition, offset, timeout_ms);

        private static Func<IntPtr, IntPtr, IntPtr, ErrorCode> _committed;
        internal static ErrorCode committed(IntPtr rk, IntPtr partitions, IntPtr timeout_ms)
            => _committed(rk, partitions, timeout_ms);

        private static Func<IntPtr, IntPtr, ErrorCode> _position;
        internal static ErrorCode position(IntPtr rk, IntPtr partitions)
            => _position(rk, partitions);

        /// <summary>
        ///     Var-arg tag types, used in producev
        /// </summary>
        internal enum ProduceVarTag
        {
            End,       // va-arg sentinel
            Topic,     // (const char *) Topic name
            Rkt,       // (rd_kafka_topic_t *) Topic handle
            Partition, // (int32_t) Partition
            Value,     // (void *, size_t) Message value (payload)
            Key,       // (void *, size_t) Message key
            Opaque,    // (void *) Application opaque
            MsgFlags,  // (int) RD_KAFKA_MSG_F_.. flags
            Timestamp, // (int64_t) Milliseconds since epoch UTC
            Header,    // (const char *, const void *, ssize_t) Message Header
            Headers,   // (rd_kafka_headers_t *) Headers list
        }

        private delegate ErrorCode Producev(IntPtr rk,
                ProduceVarTag topicTag, string topic,
                ProduceVarTag partitionTag, int partition,
                ProduceVarTag vaTag, IntPtr val, UIntPtr len,
                ProduceVarTag keyTag, IntPtr key, UIntPtr keylen,
                ProduceVarTag msgflagsTag, IntPtr msgflags,
                ProduceVarTag msg_opaqueTag, IntPtr msg_opaque,
                ProduceVarTag timestampTag, long timestamp,
                ProduceVarTag headersTag, IntPtr headers,
                ProduceVarTag endTag);
        private static Producev _producev;
        internal static ErrorCode producev(
                IntPtr rk,
                string topic,
                int partition,
                IntPtr msgflags,
                IntPtr val, UIntPtr len,
                IntPtr key, UIntPtr keylen,
                long timestamp,
                IntPtr headers,
                IntPtr msg_opaque)
            => _producev(rk,
                    ProduceVarTag.Topic, topic,
                    ProduceVarTag.Partition, partition,
                    ProduceVarTag.Value, val, len,
                    ProduceVarTag.Key, key, keylen,
                    ProduceVarTag.Opaque, msg_opaque,
                    ProduceVarTag.MsgFlags, msgflags,
                    ProduceVarTag.Timestamp, timestamp,
                    ProduceVarTag.Headers, headers,
                    ProduceVarTag.End);

        private delegate ErrorCode Flush(IntPtr rk, IntPtr timeout_ms);
        private static Flush _flush;
        internal static ErrorCode flush(IntPtr rk, IntPtr timeout_ms)
            => _flush(rk, timeout_ms);

        private delegate ErrorCode Metadata(IntPtr rk, bool all_topics,
                IntPtr only_rkt, out IntPtr metadatap, IntPtr timeout_ms);
        private static Metadata _metadata;
        internal static ErrorCode metadata(IntPtr rk, bool all_topics,
                IntPtr only_rkt, out IntPtr metadatap, IntPtr timeout_ms)
            => _metadata(rk, all_topics, only_rkt, out metadatap, timeout_ms);

        private static Action<IntPtr> _metadata_destroy;
        internal static void metadata_destroy(IntPtr metadata)
            => _metadata_destroy(metadata);

        private delegate ErrorCode ListGroups(IntPtr rk, string group,
                out IntPtr grplistp, IntPtr timeout_ms);
        private static ListGroups _list_groups;
        internal static ErrorCode list_groups(IntPtr rk, string group,
                out IntPtr grplistp, IntPtr timeout_ms)
            => _list_groups(rk, group, out grplistp, timeout_ms);

        private static Action<IntPtr> _group_list_destroy;
        internal static void group_list_destroy(IntPtr grplist)
            => _group_list_destroy(grplist);

        private static Func<IntPtr, string, IntPtr> _brokers_add;
        internal static IntPtr brokers_add(IntPtr rk, string brokerlist)
            => _brokers_add(rk, brokerlist);

        private static Func<IntPtr, int> _outq_len;
        internal static int outq_len(IntPtr rk) => _outq_len(rk);



        //
        // Admin API
        //

        private static Func<IntPtr, AdminOp, IntPtr> _AdminOptions_new;
        internal static IntPtr AdminOptions_new(IntPtr rk, AdminOp op) => _AdminOptions_new(rk, op);

        private static Action<IntPtr> _AdminOptions_destroy;
        internal static void AdminOptions_destroy(IntPtr options) => _AdminOptions_destroy(options);
        
        private static Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode> _AdminOptions_set_request_timeout;
        internal static ErrorCode AdminOptions_set_request_timeout(
            IntPtr options,
            IntPtr timeout_ms,
            StringBuilder errstr,
            UIntPtr errstr_size) => _AdminOptions_set_request_timeout(options, timeout_ms, errstr, errstr_size);

        private static Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode> _AdminOptions_set_operation_timeout;
        internal static ErrorCode AdminOptions_set_operation_timeout(
            IntPtr options,
            IntPtr timeout_ms,
            StringBuilder errstr,
            UIntPtr errstr_size) => _AdminOptions_set_operation_timeout(options, timeout_ms, errstr, errstr_size);

        private static Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode> _AdminOptions_set_validate_only;
        internal static ErrorCode AdminOptions_set_validate_only(
            IntPtr options,
            IntPtr true_or_false,
            StringBuilder errstr,
            UIntPtr errstr_size) => _AdminOptions_set_validate_only(options, true_or_false, errstr, errstr_size);

        private static Func<IntPtr, IntPtr, StringBuilder, UIntPtr, ErrorCode> _AdminOptions_set_incremental;
        internal static ErrorCode AdminOptions_set_incremental(
            IntPtr options,
            IntPtr true_or_false,
            StringBuilder errstr,
            UIntPtr errstr_size) => _AdminOptions_set_incremental(options, true_or_false, errstr, errstr_size);

        private static Func<IntPtr, int, StringBuilder, UIntPtr, ErrorCode> _AdminOptions_set_broker;
        internal static ErrorCode AdminOptions_set_broker(
            IntPtr options,
            int broker_id,
            StringBuilder errstr,
            UIntPtr errstr_size) => _AdminOptions_set_broker(options, broker_id, errstr, errstr_size);

        private static Action<IntPtr, IntPtr> _AdminOptions_set_opaque;
        internal static void AdminOptions_set_opaque(
            IntPtr options,
            IntPtr opaque) => _AdminOptions_set_opaque(options, opaque);


        private static Func<string, IntPtr, IntPtr, StringBuilder, UIntPtr, IntPtr> _NewTopic_new;
        internal static IntPtr NewTopic_new(
                        string topic,
                        IntPtr num_partitions,
                        IntPtr replication_factor,
                        StringBuilder errstr,
                        UIntPtr errstr_size) => _NewTopic_new(topic, num_partitions, replication_factor, errstr, errstr_size);

        private static Action<IntPtr> _NewTopic_destroy;
        internal static void NewTopic_destroy(IntPtr new_topic) => _NewTopic_destroy(new_topic);

        private static Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode> _NewTopic_set_replica_assignment;
        internal static ErrorCode NewTopic_set_replica_assignment(
            IntPtr new_topic,
            int partition,
            int[] broker_ids,
            UIntPtr broker_id_cnt,
            StringBuilder errstr,
            UIntPtr errstr_size) => _NewTopic_set_replica_assignment(new_topic, partition, broker_ids, broker_id_cnt, errstr, errstr_size);

        private static Func<IntPtr, string, string, ErrorCode> _NewTopic_set_config;
        internal static ErrorCode NewTopic_set_config(
                        IntPtr new_topic,
                        string name,
                        string value) => _NewTopic_set_config(new_topic, name, value);


        private static Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr> _CreateTopics;
        internal static void CreateTopics(
            IntPtr rk,
            IntPtr[] new_topics,
            UIntPtr new_topic_cnt,
            IntPtr options,
            IntPtr rkqu) => _CreateTopics(rk, new_topics, new_topic_cnt, options, rkqu);

        private delegate IntPtr _CreateTopics_result_topics_delegate(IntPtr result, out UIntPtr cntp);
        private static _CreateTopics_result_topics_delegate _CreateTopics_result_topics;
        internal static IntPtr CreateTopics_result_topics(
            IntPtr result,
            out UIntPtr cntp) => _CreateTopics_result_topics(result, out cntp);


        private static Func<string, IntPtr> _DeleteTopic_new;
        internal static IntPtr DeleteTopic_new(
                string topic
        ) => _DeleteTopic_new(topic);

        private static Action<IntPtr> _DeleteTopic_destroy;
        internal static void DeleteTopic_destroy(
            IntPtr del_topic) => _DeleteTopic_destroy(del_topic);


        private static Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr> _DeleteTopics;
        internal static void DeleteTopics(
            IntPtr rk,
            IntPtr[] del_topics,
            UIntPtr del_topic_cnt,
            IntPtr options,
            IntPtr rkqu) => _DeleteTopics(rk, del_topics, del_topic_cnt, options, rkqu);


        private delegate IntPtr _DeleteTopics_result_topics_delegate(IntPtr result, out UIntPtr cntp);
        private static _DeleteTopics_result_topics_delegate _DeleteTopics_result_topics;
        internal static IntPtr DeleteTopics_result_topics(
            IntPtr result,
            out UIntPtr cntp
        ) => _DeleteTopics_result_topics(result, out cntp);


        private static Func<string, UIntPtr, StringBuilder, UIntPtr, IntPtr> _NewPartitions_new;
        internal static IntPtr NewPartitions_new(
                string topic, 
                UIntPtr new_total_cnt,
                StringBuilder errstr, UIntPtr errstr_size
                ) => _NewPartitions_new(topic, new_total_cnt, errstr, errstr_size);

        private static Action<IntPtr> _NewPartitions_destroy;
        internal static void NewPartitions_destroy(
                IntPtr new_parts) => _NewPartitions_destroy(new_parts);


        private static Func<IntPtr, int, int[], UIntPtr, StringBuilder, UIntPtr, ErrorCode> _NewPartitions_set_replica_assignment;
        internal static ErrorCode NewPartitions_set_replica_assignment(
                IntPtr new_parts,
                int new_partition_idx,
                int[] broker_ids,
                UIntPtr broker_id_cnt,
                StringBuilder errstr,
                UIntPtr errstr_size) => _NewPartitions_set_replica_assignment(new_parts, new_partition_idx, broker_ids, broker_id_cnt, errstr, errstr_size);

        private static Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr> _CreatePartitions;
        internal static void CreatePartitions(
                IntPtr rk,
                IntPtr[] new_parts,
                UIntPtr new_parts_cnt,
                IntPtr options,
                IntPtr rkqu) => _CreatePartitions(rk, new_parts, new_parts_cnt, options, rkqu);

        private delegate IntPtr _CreatePartitions_result_topics_delegate(IntPtr result, out UIntPtr cntp);
        private static _CreatePartitions_result_topics_delegate _CreatePartitions_result_topics;
        internal static IntPtr CreatePartitions_result_topics(
            IntPtr result,
            out UIntPtr cntp
        ) => _CreatePartitions_result_topics(result, out cntp);


        private static Func<ConfigSource, IntPtr> _ConfigSource_name;
        internal static IntPtr ConfigSource_name(
                ConfigSource configsource) => _ConfigSource_name(configsource);


        private static Func<IntPtr, IntPtr> _ConfigEntry_name;
        internal static IntPtr ConfigEntry_name(
                IntPtr entry) => _ConfigEntry_name(entry);

        private static Func<IntPtr, IntPtr> _ConfigEntry_value;
        internal static IntPtr ConfigEntry_value (
                IntPtr entry) => _ConfigEntry_value(entry);

        private static Func<IntPtr, ConfigSource> _ConfigEntry_source;
        internal static ConfigSource ConfigEntry_source(
                IntPtr entry) => _ConfigEntry_source(entry);

        private static Func<IntPtr, IntPtr> _ConfigEntry_is_read_only;
        internal static IntPtr ConfigEntry_is_read_only(
                IntPtr entry) => _ConfigEntry_is_read_only(entry);

        private static Func<IntPtr, IntPtr> _ConfigEntry_is_default;
        internal static IntPtr ConfigEntry_is_default(
                IntPtr entry) => _ConfigEntry_is_default(entry);

        private static Func<IntPtr, IntPtr> _ConfigEntry_is_sensitive;
        internal static IntPtr ConfigEntry_is_sensitive(
                IntPtr entry) => _ConfigEntry_is_sensitive(entry);

        private static Func<IntPtr, IntPtr> _ConfigEntry_is_synonym;
        internal static IntPtr ConfigEntry_is_synonym (
                IntPtr entry) => _ConfigEntry_is_synonym(entry);

        private delegate IntPtr _ConfigEntry_synonyms_delegate(IntPtr entry, out UIntPtr cntp);
        private static _ConfigEntry_synonyms_delegate _ConfigEntry_synonyms;
        internal static IntPtr ConfigEntry_synonyms(
                IntPtr entry,
                out UIntPtr cntp) => _ConfigEntry_synonyms(entry, out cntp);

        private static Func<ResourceType, IntPtr> _ResourceType_name;
        internal static IntPtr ResourceType_name(
                ResourceType restype) => _ResourceType_name(restype);

        private static Func<ResourceType, string, IntPtr> _ConfigResource_new;
        internal static IntPtr ConfigResource_new(
                ResourceType restype,
                string resname) => _ConfigResource_new(restype, resname);

        private static Action<IntPtr> _ConfigResource_destroy;
        internal static void ConfigResource_destroy(
                IntPtr config) => _ConfigResource_destroy(config);

        private static Func<IntPtr, string, string, ErrorCode> _ConfigResource_add_config;
        internal static ErrorCode ConfigResource_add_config(
                IntPtr config,
                string name, 
                string value) => _ConfigResource_add_config(config, name, value);

        private static Func<IntPtr, string, string, ErrorCode> _ConfigResource_set_config;
        internal static ErrorCode ConfigResource_set_config(
                IntPtr config,
                string name, 
                string value) => _ConfigResource_set_config(config, name, value);

        private static Func<IntPtr, string, ErrorCode> _ConfigResource_delete_config;
        internal static ErrorCode ConfigResource_delete_config(
                IntPtr config,
                string name) => _ConfigResource_delete_config(config, name);

        private delegate IntPtr _ConfigResource_configs_delegate(IntPtr config, out UIntPtr cntp);
        private static _ConfigResource_configs_delegate _ConfigResource_configs;
        internal static IntPtr ConfigResource_configs(
                IntPtr config,
                out UIntPtr cntp) => _ConfigResource_configs(config, out cntp);


        private static Func<IntPtr, ResourceType> _ConfigResource_type;
        internal static ResourceType ConfigResource_type(
                IntPtr config) => _ConfigResource_type(config);

        private static Func<IntPtr, IntPtr> _ConfigResource_name;
        internal static IntPtr ConfigResource_name(
                IntPtr config) => _ConfigResource_name(config);

        private static Func<IntPtr, ErrorCode> _ConfigResource_error;
        internal static ErrorCode ConfigResource_error(
                IntPtr config) => _ConfigResource_error(config);

        private static Func<IntPtr, IntPtr> _ConfigResource_error_string;
        internal static IntPtr ConfigResource_error_string(
                IntPtr config) => _ConfigResource_error_string(config);
        

        private static Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr> _AlterConfigs;
        internal static void AlterConfigs (
                IntPtr rk,
                IntPtr[] configs,
                UIntPtr config_cnt,
                IntPtr options,
                IntPtr rkqu) => _AlterConfigs(rk, configs, config_cnt, options, rkqu);

        private delegate IntPtr _AlterConfigs_result_resources_delegate(IntPtr result, out UIntPtr cntp);
        private static _AlterConfigs_result_resources_delegate _AlterConfigs_result_resources;
        internal static IntPtr AlterConfigs_result_resources(
                IntPtr result,
                out UIntPtr cntp) => _AlterConfigs_result_resources(result, out cntp);

        private static Action<IntPtr, IntPtr[], UIntPtr, IntPtr, IntPtr> _DescribeConfigs;
        internal static void DescribeConfigs (
                IntPtr rk,
                IntPtr[] configs,
                UIntPtr config_cnt,
                IntPtr options,
                IntPtr rkqu) => _DescribeConfigs(rk, configs, config_cnt, options, rkqu);

        private delegate IntPtr _DescribeConfigs_result_resources_delegate(IntPtr result, out UIntPtr cntp);
        private static _DescribeConfigs_result_resources_delegate _DescribeConfigs_result_resources;
        internal static IntPtr DescribeConfigs_result_resources(
                IntPtr result,
                out UIntPtr cntp) => _DescribeConfigs_result_resources(result, out cntp);



        private static Func<IntPtr, ErrorCode> _topic_result_error;
        internal static ErrorCode topic_result_error(IntPtr topicres) => _topic_result_error(topicres);

        private static Func<IntPtr, IntPtr> _topic_result_error_string;
        internal static IntPtr topic_result_error_string(IntPtr topicres) => _topic_result_error_string(topicres);

        private static Func<IntPtr, IntPtr> _topic_result_name;
        internal static IntPtr topic_result_name(IntPtr topicres) => _topic_result_name(topicres);


        //
        // Queues
        //

        private static Func<IntPtr, IntPtr> _queue_new;
        internal static IntPtr queue_new(IntPtr rk)
            => _queue_new(rk);

        private static Action<IntPtr> _queue_destroy;
        internal static void queue_destroy(IntPtr rkqu)
            => _queue_destroy(rkqu);

        private static Func<IntPtr, IntPtr, IntPtr> _queue_poll;
        internal static IntPtr queue_poll(IntPtr rkqu, int timeout_ms)
            => _queue_poll(rkqu, (IntPtr)timeout_ms);


        //
        // Events
        //

        private static Action<IntPtr> _event_destroy;
        internal static void event_destroy(IntPtr rkev)
            => _event_destroy(rkev);

        private static Func<IntPtr, IntPtr> _event_opaque;
        internal static IntPtr event_opaque(IntPtr rkev)
            => _event_opaque(rkev);

        private static Func<IntPtr, EventType> _event_type;
        internal static EventType event_type(IntPtr rkev)
            => _event_type(rkev);

        private static Func<IntPtr, ErrorCode> _event_error;
        internal static ErrorCode event_error(IntPtr rkev)
            => _event_error(rkev);

        private static Func<IntPtr, IntPtr> _event_error_string;
        internal static string event_error_string(IntPtr rkev)
            => Util.Marshal.PtrToStringUTF8(_event_error_string(rkev));

        private static Func<IntPtr, IntPtr> _event_topic_partition_list;
        internal static IntPtr event_topic_partition_list(IntPtr rkev)
            => _event_topic_partition_list(rkev);


        //
        // error_t
        //

        private static Func<IntPtr, ErrorCode> _error_code;
        internal static ErrorCode error_code(IntPtr error)
            => _error_code(error);

        private static Func<IntPtr, IntPtr> _error_string;
        internal static string error_string(IntPtr error)
            => Util.Marshal.PtrToStringUTF8(_error_string(error));

        private static Func<IntPtr, IntPtr> _error_is_fatal;
        internal static bool error_is_fatal(IntPtr error)
            => _error_is_fatal(error) != IntPtr.Zero;

        private static Func<IntPtr, IntPtr> _error_is_retriable;
        internal static bool error_is_retriable(IntPtr error)
            => _error_is_retriable(error) != IntPtr.Zero;

        private static Func<IntPtr, IntPtr> _error_txn_requires_abort;
        internal static bool error_txn_requires_abort(IntPtr error)
            => _error_txn_requires_abort(error) != IntPtr.Zero;

        private static Action<IntPtr> _error_destroy;
        internal static void error_destroy(IntPtr error)
            => _error_destroy(error);
    }
}
