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
using System.IO;
using System.Text;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;
#if NET45 || NET46
using System.Reflection;
using System.ComponentModel;
#endif


namespace Confluent.Kafka.Impl
{
    internal static class LibRdKafka
    {
        // min librdkafka version, to change when binding to new function are added
        const long minVersion = 0x000903ff;

        // max length for error strings built by librdkafka
        internal const int MaxErrorStringLength = 512;

#if NET45 || NET46
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
        private static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, LoadLibraryFlags dwFlags);

        [DllImport("kernel32", SetLastError = true)]
        private static extern IntPtr GetModuleHandle(string lpFileName);

        static void LoadNet45Librdkafka()
        {
            // in net45, librdkafka.dll is not in the process directory, we have to load it manually
            // and also search in the same folder for its dependencies (LOAD_WITH_ALTERED_SEARCH_PATH)
            var is64 = IntPtr.Size == 8;
            var baseUri = new Uri(Assembly.GetExecutingAssembly().GetName().EscapedCodeBase);
            var baseDirectory = Path.GetDirectoryName(baseUri.LocalPath);
            var dllDirectory = Path.Combine(baseDirectory, is64 ? "x64" : "x86");

            if (LoadLibraryEx(Path.Combine(dllDirectory, "librdkafka.dll"),
                IntPtr.Zero, LoadLibraryFlags.LOAD_WITH_ALTERED_SEARCH_PATH) == IntPtr.Zero)
            {
                //catch the last win32 error by default and keep the associated default message
                var win32Exception = new Win32Exception();
                var additionalMessage =
                    $"Error while loading librdkafka.dll or its dependencies from {dllDirectory}. " +
                    $"Check the directory exists, if not check your deployment process. " +
                    $"You can also load the library and its dependencies by yourself " +
                    $"before any call to Confluent.Kafka";

                throw new InvalidOperationException(additionalMessage, win32Exception);
            }
        }
#endif

        static LibRdKafka()
        {
#if NET45 || NET46
            try
            {
                // In net45, we don't put the native dlls in the assembly directory
                // but in subfolders x64 and x86
                // we have to force the load as it won't be found by the system by itself
                if (GetModuleHandle(NativeMethods.DllName) == IntPtr.Zero)
                {
                    // In some cases, the user might want to load the library by himself
                    // (if he could not have the folders in the bin directory, or to change the version of the dll)
                    // If he did it before first call to Confluent.Kafka, GetModuleHandle will be nonZero
                    // and we don't have to force the load (which would fail and throw otherwise)
                    LoadNet45Librdkafka();
                }
            }
            catch
            {
                // There may be some platforms where GetModuleHandle/LoadLibraryEx do not work
                // Fail silently so the user can still have the opportunity to load the dll by himself
            }
#endif
            _version = NativeMethods.rd_kafka_version;
            _version_str = NativeMethods.rd_kafka_version_str;
            _get_debug_contexts = NativeMethods.rd_kafka_get_debug_contexts;
            _err2str = NativeMethods.rd_kafka_err2str;
            _last_error = NativeMethods.rd_kafka_last_error;
            _topic_partition_list_new = NativeMethods.rd_kafka_topic_partition_list_new;
            _topic_partition_list_destroy = NativeMethods.rd_kafka_topic_partition_list_destroy;
            _topic_partition_list_add = NativeMethods.rd_kafka_topic_partition_list_add;
            _message_timestamp = NativeMethods.rd_kafka_message_timestamp;
            _message_destroy = NativeMethods.rd_kafka_message_destroy;
            _conf_new = NativeMethods.rd_kafka_conf_new;
            _conf_destroy = NativeMethods.rd_kafka_conf_destroy;
            _conf_dup = NativeMethods.rd_kafka_conf_dup;
            _conf_set = NativeMethods.rd_kafka_conf_set;
            _conf_set_dr_msg_cb = NativeMethods.rd_kafka_conf_set_dr_msg_cb;
            _conf_set_rebalance_cb = NativeMethods.rd_kafka_conf_set_rebalance_cb;
            _conf_set_error_cb = NativeMethods.rd_kafka_conf_set_error_cb;
            _conf_set_offset_commit_cb = NativeMethods.rd_kafka_conf_set_offset_commit_cb;
            _conf_set_log_cb = NativeMethods.rd_kafka_conf_set_log_cb;
            _conf_set_stats_cb = NativeMethods.rd_kafka_conf_set_stats_cb;
            _conf_set_default_topic_conf = NativeMethods.rd_kafka_conf_set_default_topic_conf;
            _conf_get = NativeMethods.rd_kafka_conf_get;
            _topic_conf_get = NativeMethods.rd_kafka_topic_conf_get;
            _conf_dump = NativeMethods.rd_kafka_conf_dump;
            _topic_conf_dump = NativeMethods.rd_kafka_topic_conf_dump;
            _conf_dump_free = NativeMethods.rd_kafka_conf_dump_free;
            _topic_conf_new = NativeMethods.rd_kafka_topic_conf_new;
            _topic_conf_dup = NativeMethods.rd_kafka_topic_conf_dup;
            _topic_conf_destroy = NativeMethods.rd_kafka_topic_conf_destroy;
            _topic_conf_set = NativeMethods.rd_kafka_topic_conf_set;
            _topic_conf_set_partitioner_cb = NativeMethods.rd_kafka_topic_conf_set_partitioner_cb;
            _topic_partition_available = NativeMethods.rd_kafka_topic_partition_available;
            _new = NativeMethods.rd_kafka_new;
            _destroy = NativeMethods.rd_kafka_destroy;
            _name = NativeMethods.rd_kafka_name;
            _memberid = NativeMethods.rd_kafka_memberid;
            _topic_new = NativeMethods.rd_kafka_topic_new;
            _topic_destroy = NativeMethods.rd_kafka_topic_destroy;
            _topic_name = NativeMethods.rd_kafka_topic_name;
            _poll = NativeMethods.rd_kafka_poll;
            _poll_set_consumer = NativeMethods.rd_kafka_poll_set_consumer;
            _query_watermark_offsets = NativeMethods.rd_kafka_query_watermark_offsets;
            _get_watermark_offsets = NativeMethods.rd_kafka_get_watermark_offsets;
            _offsets_for_times = NativeMethods.rd_kafka_offsets_for_times;
            _mem_free = NativeMethods.rd_kafka_mem_free;
            _subscribe = NativeMethods.rd_kafka_subscribe;
            _unsubscribe = NativeMethods.rd_kafka_unsubscribe;
            _subscription = NativeMethods.rd_kafka_subscription;
            _consumer_poll = NativeMethods.rd_kafka_consumer_poll;
            _consumer_close = NativeMethods.rd_kafka_consumer_close;
            _assign = NativeMethods.rd_kafka_assign;
            _assignment = NativeMethods.rd_kafka_assignment;
            _commit = NativeMethods.rd_kafka_commit;
            _commit_queue = NativeMethods.rd_kafka_commit_queue;
            _committed = NativeMethods.rd_kafka_committed;
            _position = NativeMethods.rd_kafka_position;
            _producev = NativeMethods.rd_kafka_producev;
            _flush = NativeMethods.rd_kafka_flush;
            _metadata = NativeMethods.rd_kafka_metadata;
            _metadata_destroy = NativeMethods.rd_kafka_metadata_destroy;
            _list_groups = NativeMethods.rd_kafka_list_groups;
            _group_list_destroy = NativeMethods.rd_kafka_group_list_destroy;
            _brokers_add = NativeMethods.rd_kafka_brokers_add;
            _outq_len = NativeMethods.rd_kafka_outq_len;
            _queue_new = NativeMethods.rd_kafka_queue_new;
            _queue_destroy = NativeMethods.rd_kafka_queue_destroy;
            _event_type = NativeMethods.rd_kafka_event_type;
            _event_error = NativeMethods.rd_kafka_event_error;
            _event_error_string = NativeMethods.rd_kafka_event_error_string;
            _event_topic_partition_list = NativeMethods.rd_kafka_event_topic_partition_list;
            _event_destroy = NativeMethods.rd_kafka_event_destroy;
            _queue_poll = NativeMethods.rd_kafka_queue_poll;

            if ((long)version() < minVersion)
            {
                throw new FileLoadException($"Invalid librdkafka version {(long)version():x}, expected at least {minVersion:x}");
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
        internal delegate void LogDelegate(IntPtr rk, int level, string fac, string buf);

        [UnmanagedFunctionPointer(callingConvention: CallingConvention.Cdecl)]
        internal delegate int StatsDelegate(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque);

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

        private static Func<ErrorCode> _last_error;
        internal static ErrorCode last_error() => _last_error();

        internal delegate long messageTimestampDelegate(IntPtr rkmessage, out IntPtr tstype);
        private static messageTimestampDelegate _message_timestamp;
        internal static long message_timestamp(IntPtr rkmessage, out IntPtr tstype) => _message_timestamp(rkmessage, out tstype);

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

        private static Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle> _new;
        internal static SafeKafkaHandle kafka_new(RdKafkaType type, IntPtr conf,
                StringBuilder errstr, UIntPtr errstr_size)
            => _new(type, conf, errstr, errstr_size);

        private static Action<IntPtr> _destroy;
        internal static void destroy(IntPtr rk) => _destroy(rk);

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

        private static Func<IntPtr, IntPtr, bool, ErrorCode> _commit;
        internal static ErrorCode commit(IntPtr rk, IntPtr offsets, bool async)
            => _commit(rk, offsets, async);

        private static Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode> _commit_queue;
        internal static ErrorCode commit_queue(IntPtr rk, IntPtr offsets, IntPtr rkqu,
            CommitDelegate cb, IntPtr opaque)
            => _commit_queue(rk, offsets, rkqu, cb, opaque);

        private static Func<IntPtr, IntPtr, IntPtr, ErrorCode> _committed;
        internal static ErrorCode committed(IntPtr rk, IntPtr partitions, IntPtr timeout_ms)
            => _committed(rk, partitions, timeout_ms);

        private static Func<IntPtr, IntPtr, ErrorCode> _position;
        internal static ErrorCode position(IntPtr rk, IntPtr partitions)
            => _position(rk, partitions);

        /// <summary>
        ///     Var-arg tag types, used in producev
        /// </summary>
        private enum ProduceVarTag
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
        }

        private delegate ErrorCode Producev(IntPtr rk,
                ProduceVarTag topicTag, string topic,
                ProduceVarTag partitionTag, int partition,
                ProduceVarTag vaTag, IntPtr val, UIntPtr len,
                ProduceVarTag keyTag, IntPtr key, UIntPtr keylen,
                ProduceVarTag msgflagsTag, IntPtr msgflags,
                ProduceVarTag msg_opaqueTag, IntPtr msg_opaque,
                ProduceVarTag timestampTag, long timestamp,
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
                IntPtr msg_opaque)
            => _producev(rk,
                    ProduceVarTag.Topic, topic,
                    ProduceVarTag.Partition, partition,
                    ProduceVarTag.Value, val, len,
                    ProduceVarTag.Key, key, keylen,
                    ProduceVarTag.Opaque, msg_opaque,
                    ProduceVarTag.MsgFlags, msgflags,
                    ProduceVarTag.Timestamp, timestamp,
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

        private static Func<IntPtr, int> _event_type;
        internal static int event_type(IntPtr rkev)
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



        private class NativeMethods
        {
            public const string DllName = "librdkafka";

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
            internal static extern /* int64_t */ long rd_kafka_message_timestamp(
                    /* rd_kafka_message_t * */ IntPtr rkmessage,
                    /* r_kafka_timestamp_type_t * */ out IntPtr tstype);

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
                    DeliveryReportDelegate dr_msg_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_rebalance_cb(
                    IntPtr conf, RebalanceDelegate rebalance_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_offset_commit_cb(
                    IntPtr conf, CommitDelegate commit_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_error_cb(
                    IntPtr conf, ErrorDelegate error_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_log_cb(IntPtr conf, LogDelegate log_cb);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern void rd_kafka_conf_set_stats_cb(IntPtr conf, StatsDelegate stats_cb);

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
                    IntPtr topic_conf, PartitionerDelegate partitioner_cb);

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
            internal static extern ErrorCode rd_kafka_commit(
                    IntPtr rk,
                    /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                    bool async);

            [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
            internal static extern ErrorCode rd_kafka_commit_queue(
                    IntPtr rk,
                    /* const rd_kafka_topic_partition_list_t * */ IntPtr offsets,
                    /* rd_kafka_queue_t * */ IntPtr rkqu,
                    /* offset_commit_cb * */ CommitDelegate cb,
                    /* void * */ IntPtr opaque);

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
                ProduceVarTag topicType, [MarshalAs(UnmanagedType.LPStr)] string topic,
                ProduceVarTag partitionType, int partition,
                ProduceVarTag vaType, IntPtr val, UIntPtr len,
                ProduceVarTag keyType, IntPtr key, UIntPtr keylen,
                ProduceVarTag msgflagsType, IntPtr msgflags,
                ProduceVarTag msg_opaqueType, IntPtr msg_opaque,
                ProduceVarTag timestampType, long timestamp,
                ProduceVarTag endType);

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
}
