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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Impl.NativeMethods;
using System.Reflection;
#if NET45 || NET46 || NET47
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

            _version = (Func<IntPtr>)methods.Where(m => m.Name == "rd_kafka_version").Single().CreateDelegate(typeof(Func<IntPtr>));
            _version_str = (Func<IntPtr>)methods.Where(m => m.Name == "rd_kafka_version_str").Single().CreateDelegate(typeof(Func<IntPtr>));
            _get_debug_contexts = (Func<IntPtr>)methods.Where(m => m.Name == "rd_kafka_get_debug_contexts").Single().CreateDelegate(typeof(Func<IntPtr>));
            _err2str = (Func<ErrorCode, IntPtr>)methods.Where(m => m.Name == "rd_kafka_err2str").Single().CreateDelegate(typeof(Func<ErrorCode, IntPtr>));
            _last_error = (Func<ErrorCode>)methods.Where(m => m.Name == "rd_kafka_last_error").Single().CreateDelegate(typeof(Func<ErrorCode>));
            _topic_partition_list_new = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_partition_list_new").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_partition_list_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_partition_list_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _topic_partition_list_add = (Func<IntPtr, string, int, IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_partition_list_add").Single().CreateDelegate(typeof(Func<IntPtr, string, int, IntPtr>));
            _message_timestamp = (messageTimestampDelegate)methods.Where(m => m.Name == "rd_kafka_message_timestamp").Single().CreateDelegate(typeof(messageTimestampDelegate));
            _message_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_message_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _conf_new = (Func<SafeConfigHandle>)methods.Where(m => m.Name == "rd_kafka_conf_new").Single().CreateDelegate(typeof(Func<SafeConfigHandle>));
            _conf_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_conf_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _conf_dup = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_conf_dup").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _conf_set = (Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>)methods.Where(m => m.Name == "rd_kafka_conf_set").Single().CreateDelegate(typeof(Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>));
            _conf_set_dr_msg_cb = (Action<IntPtr, DeliveryReportDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_dr_msg_cb").Single().CreateDelegate(typeof(Action<IntPtr, DeliveryReportDelegate>));
            _conf_set_rebalance_cb = (Action<IntPtr, RebalanceDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_rebalance_cb").Single().CreateDelegate(typeof(Action<IntPtr, RebalanceDelegate>));
            _conf_set_error_cb = (Action<IntPtr, ErrorDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_error_cb").Single().CreateDelegate(typeof(Action<IntPtr, ErrorDelegate>));
            _conf_set_offset_commit_cb = (Action<IntPtr, CommitDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_offset_commit_cb").Single().CreateDelegate(typeof(Action<IntPtr, CommitDelegate>));
            _conf_set_log_cb = (Action<IntPtr, LogDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_log_cb").Single().CreateDelegate(typeof(Action<IntPtr, LogDelegate>));
            _conf_set_stats_cb = (Action<IntPtr, StatsDelegate>)methods.Where(m => m.Name == "rd_kafka_conf_set_stats_cb").Single().CreateDelegate(typeof(Action<IntPtr, StatsDelegate>));
            _conf_set_default_topic_conf = (Action<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_conf_set_default_topic_conf").Single().CreateDelegate(typeof(Action<IntPtr, IntPtr>));
            _conf_get = (ConfGet)methods.Where(m => m.Name == "rd_kafka_conf_get").Single().CreateDelegate(typeof(ConfGet));
            _topic_conf_get = (ConfGet)methods.Where(m => m.Name == "rd_kafka_topic_conf_get").Single().CreateDelegate(typeof(ConfGet));
            _conf_dump = (ConfDump)methods.Where(m => m.Name == "rd_kafka_conf_dump").Single().CreateDelegate(typeof(ConfDump));
            _topic_conf_dump = (ConfDump)methods.Where(m => m.Name == "rd_kafka_topic_conf_dump").Single().CreateDelegate(typeof(ConfDump));
            _conf_dump_free = (Action<IntPtr, UIntPtr>)methods.Where(m => m.Name == "rd_kafka_conf_dump_free").Single().CreateDelegate(typeof(Action<IntPtr, UIntPtr>));
            _topic_conf_new = (Func<SafeTopicConfigHandle>)methods.Where(m => m.Name == "rd_kafka_topic_conf_new").Single().CreateDelegate(typeof(Func<SafeTopicConfigHandle>));
            _topic_conf_dup = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_conf_dup").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_conf_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_conf_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _topic_conf_set = (Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>)methods.Where(m => m.Name == "rd_kafka_topic_conf_set").Single().CreateDelegate(typeof(Func<IntPtr, string, string, StringBuilder, UIntPtr, ConfRes>));
            _topic_conf_set_partitioner_cb = (Action<IntPtr, PartitionerDelegate>)methods.Where(m => m.Name == "rd_kafka_topic_conf_set_partitioner_cb").Single().CreateDelegate(typeof(Action<IntPtr, PartitionerDelegate>));
            _topic_partition_available = (Func<IntPtr, int, bool>)methods.Where(m => m.Name == "rd_kafka_topic_partition_available").Single().CreateDelegate(typeof(Func<IntPtr, int, bool>));
            _new = (Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle>)methods.Where(m => m.Name == "rd_kafka_new").Single().CreateDelegate(typeof(Func<RdKafkaType, IntPtr, StringBuilder, UIntPtr, SafeKafkaHandle>));
            _name = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_name").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _memberid = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_memberid").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _topic_new = (Func<IntPtr, string, IntPtr, SafeTopicHandle>)methods.Where(m => m.Name == "rd_kafka_topic_new").Single().CreateDelegate(typeof(Func<IntPtr, string, IntPtr, SafeTopicHandle>));
            _topic_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _topic_name = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_topic_name").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_poll").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _poll_set_consumer = (Func<IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_poll_set_consumer").Single().CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _query_watermark_offsets = (QueryOffsets)methods.Where(m => m.Name == "rd_kafka_query_watermark_offsets").Single().CreateDelegate(typeof(QueryOffsets));
            _get_watermark_offsets = (GetOffsets)methods.Where(m => m.Name == "rd_kafka_get_watermark_offsets").Single().CreateDelegate(typeof(GetOffsets));
            _offsets_for_times = (OffsetsForTimes)methods.Where(m => m.Name == "rd_kafka_offsets_for_times").Single().CreateDelegate(typeof(OffsetsForTimes));
            _mem_free = (Action<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_mem_free").Single().CreateDelegate(typeof(Action<IntPtr, IntPtr>));
            _subscribe = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_subscribe").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _unsubscribe = (Func<IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_unsubscribe").Single().CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _subscription = (Subscription)methods.Where(m => m.Name == "rd_kafka_subscription").Single().CreateDelegate(typeof(Subscription));
            _consumer_poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_consumer_poll").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));
            _consumer_close = (Func<IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_consumer_close").Single().CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _assign = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_assign").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _assignment = (Assignment)methods.Where(m => m.Name == "rd_kafka_assignment").Single().CreateDelegate(typeof(Assignment));
            _offsets_store = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_offsets_store").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _commit = (Func<IntPtr, IntPtr, bool, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_commit").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, bool, ErrorCode>));
            _commit_queue = (Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_commit_queue").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, CommitDelegate, IntPtr, ErrorCode>));
            _committed = (Func<IntPtr, IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_committed").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr, ErrorCode>));
            _pause_partitions = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_pause_partitions").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _resume_partitions = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_resume_partitions").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _seek = (Func<IntPtr, int, long, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_seek").Single().CreateDelegate(typeof(Func<IntPtr, int, long, IntPtr, ErrorCode>));
            _position = (Func<IntPtr, IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_position").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, ErrorCode>));
            _producev = (Producev)methods.Where(m => m.Name == "rd_kafka_producev").Single().CreateDelegate(typeof(Producev));
            _flush = (Flush)methods.Where(m => m.Name == "rd_kafka_flush").Single().CreateDelegate(typeof(Flush));
            _metadata = (Metadata)methods.Where(m => m.Name == "rd_kafka_metadata").Single().CreateDelegate(typeof(Metadata));
            _metadata_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_metadata_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _list_groups = (ListGroups)methods.Where(m => m.Name == "rd_kafka_list_groups").Single().CreateDelegate(typeof(ListGroups));
            _group_list_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_group_list_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _brokers_add = (Func<IntPtr, string, IntPtr>)methods.Where(m => m.Name == "rd_kafka_brokers_add").Single().CreateDelegate(typeof(Func<IntPtr, string, IntPtr>));
            _outq_len = (Func<IntPtr, int>)methods.Where(m => m.Name == "rd_kafka_outq_len").Single().CreateDelegate(typeof(Func<IntPtr, int>));
            _queue_new = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_queue_new").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _queue_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_queue_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _event_type = (Func<IntPtr, int>)methods.Where(m => m.Name == "rd_kafka_event_type").Single().CreateDelegate(typeof(Func<IntPtr, int>));
            _event_error = (Func<IntPtr, ErrorCode>)methods.Where(m => m.Name == "rd_kafka_event_error").Single().CreateDelegate(typeof(Func<IntPtr, ErrorCode>));
            _event_error_string = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_event_error_string").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _event_topic_partition_list = (Func<IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_event_topic_partition_list").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr>));
            _event_destroy = (Action<IntPtr>)methods.Where(m => m.Name == "rd_kafka_event_destroy").Single().CreateDelegate(typeof(Action<IntPtr>));
            _queue_poll = (Func<IntPtr, IntPtr, IntPtr>)methods.Where(m => m.Name == "rd_kafka_queue_poll").Single().CreateDelegate(typeof(Func<IntPtr, IntPtr, IntPtr>));

            _destroyMethodInfo = methods.Where(m => m.Name == "rd_kafka_destroy").Single();
            _destroy = (Action<IntPtr>)_destroyMethodInfo.CreateDelegate(typeof(Action<IntPtr>));

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
                                ? @"runtimes\win7-x64\native"
                                : @"runtimes\win7-x86\native");
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
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods));
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods_Debian9));
                        nativeMethodTypes.Add(typeof(NativeMethods.NativeMethods_Centos7));
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

                // Protect the _destroy and _destroyMethodInfo objects from garbage collection. This is
                // required since the Producer/Consumer finalizers may reference them, and they might
                // have otherwise been cleaned up at that point. To keep things simple, there is no reference
                // counting / corresponding Free() call - there is negligible overhead in keeping these
                // references around for the lifetime of the process.
                GCHandle.Alloc(_destroy, GCHandleType.Normal);
                GCHandle.Alloc(_destroyMethodInfo, GCHandleType.Normal);

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

        private static MethodInfo _destroyMethodInfo;
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
    }
}
