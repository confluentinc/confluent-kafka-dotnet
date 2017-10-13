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
using System.Reflection;
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

<<<<<<< HEAD
        // max length for error strings built by librdkafka
        internal const int MaxErrorStringLength = 512;

#if NET45 || NET46
=======
>>>>>>> load libraries dynamically
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

        private static class WindowsNative
        {
            [DllImport("kernel32", SetLastError = true)]
            public static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, LoadLibraryFlags dwFlags);

            [DllImport("kernel32", SetLastError = true)]
            public static extern IntPtr GetModuleHandle(string lpFileName);
        }

        private static class LinuxNative
        {
            [DllImport("libdl.so")]
            public static extern IntPtr dlopen(String fileName, int flags);

            [DllImport("libdl.so")]
            public static extern IntPtr dlerror();

            [DllImport("libdl.so")]
            public static extern IntPtr dlsym(IntPtr handle, String symbol);
        }

        private static class MacNative
        {
            [DllImport("libdl.dylib")]
            public static extern IntPtr dlopen(String fileName, int flags);

            [DllImport("libdl.dylib")]
            public static extern IntPtr dlerror();

            [DllImport("libdl.dylib")]
            public static extern IntPtr dlsym(IntPtr handle, String symbol);
        }


        public delegate IntPtr LibraryOpenDelegate(String fileName);

        public delegate IntPtr SymbolLookupDelegate(IntPtr addr, string name);

#if NET45 || NET46
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
#else

        static IntPtr TryLoadLibrdkafka(string librdkafkaName, string librarySuffix, LibraryOpenDelegate openLib)
        {
            string confluentKafkaAssemblyLocation = typeof(Error).GetTypeInfo().Assembly.Location;
            if (!confluentKafkaAssemblyLocation.EndsWith("Confluent.Kafka.dll"))
            {
                throw new Exception("unexpected Assembly name");
            }

            // In general, the librdkafka shared library we want will not be in the library load path, so we need to 
            // specify an absolute path. Unfortunately, it's relative position to Confluent.Kafka.dll (which is easy 
            // to know), is not fixed. In dotnet core this is different depending on whether the project is published 
            // or not. Here we do some magic to guess where librdkafka is is and fall back to assuming it's in the 
            // library load path if we detect an unusual situation.

            string librdkafkaLocation = null;
            if (confluentKafkaAssemblyLocation.Contains("/netcoreapp"))
            {
                librdkafkaLocation = 
                    confluentKafkaAssemblyLocation.Substring(0, confluentKafkaAssemblyLocation.Length - "/Confluent.Kafka.dll".Length) +
                    librdkafkaName + librarySuffix;
            }
            else if (confluentKafkaAssemblyLocation.Contains("/packages/librdkafka.redist"))
            {
                // TODO: maybe we should make the layout in the nuget package flat for debian, redhat etc.
                // Else, todo: work out how to know that. 
                librdkafkaLocation = "TODO - how do you reliably get distro?";
            }
            
            if (librdkafkaLocation == null || !File.Exists(librdkafkaLocation))
            {
                // don't specify absolute path - assume library is in the library load path.
                librdkafkaLocation = librdkafkaName + librarySuffix;
            }

            return openLib(librdkafkaLocation);
        }

        static void LoadLibrdkafka()
        {
            const int RTLD_NOW = 2;

            LibraryOpenDelegate openLib = null;
            SymbolLookupDelegate lookupSymbol = null;
            string librarySufix = null;

            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                openLib = (string name) => MacNative.dlopen(name, RTLD_NOW);
                lookupSymbol = MacNative.dlsym;
                librarySufix = ".dylib";
            } 
            else if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                openLib = (string name) => LinuxNative.dlopen(name, RTLD_NOW);
                lookupSymbol = LinuxNative.dlsym;
                librarySufix = ".so";
            }
            else if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                librarySufix = ".dll";
            }
            else
            {
                throw new Exception("Unsupported platform");
            }

            // TODO: allow this to be overriden with a prop specified in consumer or producer constructor.
            string[] librdkafkaNames = new string[] { "librdkafka-dep-libssl1.0.0-libsasl", "librdkafka-dep-libssl", "librdkafka-bare", "librdkafka" };

            IntPtr addr = IntPtr.Zero;
            foreach (var librdkafkaName in librdkafkaNames)
            {
                addr = TryLoadLibrdkafka(librdkafkaName, librarySufix, openLib);
                if (addr != IntPtr.Zero) 
                {
                    break;
                }
            }

            InitializeDelegates(addr, lookupSymbol);
        }
#endif

        static void InitializeDelegates(IntPtr librdkafkaAddr, SymbolLookupDelegate lookup)
        {
            version = Marshal.GetDelegateForFunctionPointer<version_delegate>(lookup(librdkafkaAddr, "rd_kafka_version"));
            version_str = Marshal.GetDelegateForFunctionPointer<version_str_delegate>(lookup(librdkafkaAddr, "rd_kafka_version_str"));
            get_debug_contexts = Marshal.GetDelegateForFunctionPointer<get_debug_contexts_delegate>(lookup(librdkafkaAddr, "rd_kafka_get_debug_contexts"));
            err2str = Marshal.GetDelegateForFunctionPointer<err2str_delegate>(lookup(librdkafkaAddr, "rd_kafka_err2str"));
            topic_partition_list_new = Marshal.GetDelegateForFunctionPointer<topic_partition_list_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_new"));
            topic_partition_list_destroy = Marshal.GetDelegateForFunctionPointer<topic_partition_list_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_destroy"));
            topic_partition_list_add = Marshal.GetDelegateForFunctionPointer<topic_partition_list_add_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_add"));
            last_error = Marshal.GetDelegateForFunctionPointer<last_error_delegate>(lookup(librdkafkaAddr, "rd_kafka_last_error"));
            message_timestamp = Marshal.GetDelegateForFunctionPointer<messageTimestampDelegate>(lookup(librdkafkaAddr, "rd_kafka_message_timestamp"));
            message_destroy = Marshal.GetDelegateForFunctionPointer<message_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_message_destroy"));
            conf_new = Marshal.GetDelegateForFunctionPointer<conf_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_new"));
            conf_destroy = Marshal.GetDelegateForFunctionPointer<conf_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_destroy"));
            conf_dup = Marshal.GetDelegateForFunctionPointer<conf_dup_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_dup"));
            conf_set = Marshal.GetDelegateForFunctionPointer<conf_set_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set"));
            conf_set_dr_msg_cb = Marshal.GetDelegateForFunctionPointer<conf_set_dr_msg_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_dr_msg_cb"));
            conf_set_rebalance_cb = Marshal.GetDelegateForFunctionPointer<conf_set_rebalance_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_rebalance_cb"));
            conf_set_offset_commit_cb = Marshal.GetDelegateForFunctionPointer<conf_set_offset_commit_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_offset_commit_cb"));
            conf_set_error_cb = Marshal.GetDelegateForFunctionPointer<conf_set_error_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_error_cb"));
            conf_set_log_cb = Marshal.GetDelegateForFunctionPointer<conf_set_log_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_log_cb"));
            conf_set_stats_cb = Marshal.GetDelegateForFunctionPointer<conf_set_stats_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_stats_cb"));
            conf_set_default_topic_conf = Marshal.GetDelegateForFunctionPointer<conf_set_default_topic_conf_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_set_default_topic_conf"));
            conf_get = Marshal.GetDelegateForFunctionPointer<ConfGet>(lookup(librdkafkaAddr, "rd_kafka_conf_get"));
            topic_conf_get = Marshal.GetDelegateForFunctionPointer<ConfGet>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_get"));
            conf_dump = Marshal.GetDelegateForFunctionPointer<ConfDump>(lookup(librdkafkaAddr, "rd_kafka_conf_dump"));
            topic_conf_dump = Marshal.GetDelegateForFunctionPointer<ConfDump>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_dump"));
            conf_dump_free = Marshal.GetDelegateForFunctionPointer<conf_dump_free_delegate>(lookup(librdkafkaAddr, "rd_kafka_conf_dump_free"));
            topic_conf_new = Marshal.GetDelegateForFunctionPointer<topic_conf_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_new"));
            topic_conf_dup = Marshal.GetDelegateForFunctionPointer<topic_conf_dup_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_dup"));
            topic_conf_destroy = Marshal.GetDelegateForFunctionPointer<topic_conf_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_destroy"));
            topic_conf_set = Marshal.GetDelegateForFunctionPointer<topic_conf_set_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_set"));
            topic_conf_set_partitioner_cb = Marshal.GetDelegateForFunctionPointer<topic_conf_set_partitioner_cb_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_conf_set_partitioner_cb"));
            topic_partition_available = Marshal.GetDelegateForFunctionPointer<topic_partition_available_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_partition_available"));
            rd_new = Marshal.GetDelegateForFunctionPointer<rd_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_new"));
            destroy = Marshal.GetDelegateForFunctionPointer<destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_destroy"));
            name = Marshal.GetDelegateForFunctionPointer<name_delegate>(lookup(librdkafkaAddr, "rd_kafka_name"));
            memberid = Marshal.GetDelegateForFunctionPointer<memberid_delegate>(lookup(librdkafkaAddr, "rd_kafka_memberid"));
            topic_new = Marshal.GetDelegateForFunctionPointer<topic_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_new"));
            topic_destroy = Marshal.GetDelegateForFunctionPointer<topic_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_destroy"));
            topic_name = Marshal.GetDelegateForFunctionPointer<topic_name_delegate>(lookup(librdkafkaAddr, "rd_kafka_topic_name"));
            poll = Marshal.GetDelegateForFunctionPointer<poll_delegate>(lookup(librdkafkaAddr, "rd_kafka_poll"));
            poll_set_consumer = Marshal.GetDelegateForFunctionPointer<poll_set_consumer_delegate>(lookup(librdkafkaAddr, "rd_kafka_poll_set_consumer"));
            query_watermark_offsets = Marshal.GetDelegateForFunctionPointer<QueryOffsets>(lookup(librdkafkaAddr, "rd_kafka_query_watermark_offsets"));
            get_watermark_offsets = Marshal.GetDelegateForFunctionPointer<GetOffsets>(lookup(librdkafkaAddr, "rd_kafka_get_watermark_offsets"));
            mem_free = Marshal.GetDelegateForFunctionPointer<mem_free_delegate>(lookup(librdkafkaAddr, "rd_kafka_mem_free"));
            subscribe = Marshal.GetDelegateForFunctionPointer<subscribe_delegate>(lookup(librdkafkaAddr, "rd_kafka_subscribe"));
            unsubscribe = Marshal.GetDelegateForFunctionPointer<unsubscribe_delegate>(lookup(librdkafkaAddr, "rd_kafka_unsubscribe"));
            subscription = Marshal.GetDelegateForFunctionPointer<Subscription>(lookup(librdkafkaAddr, "rd_kafka_subscription"));
            consumer_poll = Marshal.GetDelegateForFunctionPointer<consumer_poll_delegate>(lookup(librdkafkaAddr, "rd_kafka_consumer_poll"));
            consumer_close = Marshal.GetDelegateForFunctionPointer<consumer_close_delegate>(lookup(librdkafkaAddr, "rd_kafka_consumer_close"));
            assign = Marshal.GetDelegateForFunctionPointer<assign_delegate>(lookup(librdkafkaAddr, "rd_kafka_assign"));
            assignment = Marshal.GetDelegateForFunctionPointer<Assignment>(lookup(librdkafkaAddr, "rd_kafka_assignment"));
            commit = Marshal.GetDelegateForFunctionPointer<commit_delegate>(lookup(librdkafkaAddr, "rd_kafka_commit"));
            commit_queue = Marshal.GetDelegateForFunctionPointer<commit_queue_delegate>(lookup(librdkafkaAddr, "rd_kafka_commit_queue"));
            committed = Marshal.GetDelegateForFunctionPointer<committed_delegate>(lookup(librdkafkaAddr, "rd_kafka_committed"));
            position = Marshal.GetDelegateForFunctionPointer<position_delegate>(lookup(librdkafkaAddr, "rd_kafka_position"));
            produce = Marshal.GetDelegateForFunctionPointer<produce_delegate>(lookup(librdkafkaAddr, "rd_kafka_produce"));
            producev = Marshal.GetDelegateForFunctionPointer<producev_delegate>(lookup(librdkafkaAddr, "rd_kafka_producev"));
            flush = Marshal.GetDelegateForFunctionPointer<Flush>(lookup(librdkafkaAddr, "rd_kafka_flush"));
            metadata = Marshal.GetDelegateForFunctionPointer<Metadata>(lookup(librdkafkaAddr, "rd_kafka_metadata"));
            metadata_destroy = Marshal.GetDelegateForFunctionPointer<metadata_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_metadata_destroy"));
            list_groups = Marshal.GetDelegateForFunctionPointer<ListGroups>(lookup(librdkafkaAddr, "rd_kafka_list_groups"));
            group_list_destroy = Marshal.GetDelegateForFunctionPointer<group_list_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_group_list_destroy"));
            brokers_add = Marshal.GetDelegateForFunctionPointer<brokers_add_delegate>(lookup(librdkafkaAddr, "rd_kafka_brokers_add"));
            outq_len = Marshal.GetDelegateForFunctionPointer<outq_len_delegate>(lookup(librdkafkaAddr, "rd_kafka_outq_len"));
            queue_new = Marshal.GetDelegateForFunctionPointer<queue_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_queue_new"));
            queue_destroy = Marshal.GetDelegateForFunctionPointer<queue_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_queue_destroy"));
            event_type = Marshal.GetDelegateForFunctionPointer<event_type_delegate>(lookup(librdkafkaAddr, "rd_kafka_event_type"));
            event_error = Marshal.GetDelegateForFunctionPointer<event_error_delegate>(lookup(librdkafkaAddr, "rd_kafka_event_error"));
            event_error_string = Marshal.GetDelegateForFunctionPointer<event_error_string_delegate>(lookup(librdkafkaAddr, "rd_kafka_event_error_string"));
            event_topic_partition_list = Marshal.GetDelegateForFunctionPointer<event_topic_partition_list_delegate>(lookup(librdkafkaAddr, "rd_kafka_event_topic_partition_list"));
            event_destroy = Marshal.GetDelegateForFunctionPointer<event_destroy_delegate>(lookup(librdkafkaAddr, "rd_kafka_event_destroy"));
            queue_poll = Marshal.GetDelegateForFunctionPointer<queue_poll_delegate>(lookup(librdkafkaAddr, "rd_kafka_queue_poll"));
        }

        static object lockObj = new object();
        static bool isInitialized = false;

        public static bool IsInitialized
        {
            get
            {
                lock (lockObj)
                {
                    return isInitialized;
                }
            }
        }

        public static void Initialize()
        {
            lock (lockObj)
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
#else
                LoadLibrdkafka();
#endif

                if ((long) version() < minVersion) {
                    throw new FileLoadException($"Invalid librdkafka version {(long)version():x}, expected at least {minVersion:x}");
                }

                isInitialized = true;
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


        internal delegate IntPtr version_delegate();
        internal static version_delegate version;

        internal delegate IntPtr version_str_delegate();
        internal static version_str_delegate version_str;

        internal delegate IntPtr get_debug_contexts_delegate();
        internal static get_debug_contexts_delegate get_debug_contexts;

        internal delegate IntPtr err2str_delegate(ErrorCode e);
        internal static err2str_delegate err2str;

        internal delegate IntPtr topic_partition_list_new_delegate(IntPtr p);
        internal static topic_partition_list_new_delegate topic_partition_list_new;

        internal delegate void topic_partition_list_destroy_delegate(IntPtr p);
        internal static topic_partition_list_destroy_delegate topic_partition_list_destroy;

        internal delegate IntPtr topic_partition_list_add_delegate(IntPtr p, string s, int i);
        internal static topic_partition_list_add_delegate topic_partition_list_add;
        
        internal delegate ErrorCode last_error_delegate();
        internal static last_error_delegate last_error;

        internal delegate long messageTimestampDelegate(IntPtr rkmessage, out IntPtr tstype);
        internal static messageTimestampDelegate message_timestamp;

        internal delegate void message_destroy_delegate(IntPtr p);
        internal static message_destroy_delegate message_destroy;

        internal delegate SafeConfigHandle conf_new_delegate();
        internal static conf_new_delegate conf_new;

        internal delegate void conf_destroy_delegate(IntPtr p);
        internal static conf_destroy_delegate conf_destroy;

        internal delegate IntPtr conf_dup_delegate(IntPtr p);
        internal static conf_dup_delegate conf_dup;

        internal delegate ConfRes conf_set_delegate(IntPtr p, string s, string s2, StringBuilder s3, UIntPtr p2);
        internal static conf_set_delegate conf_set;
        
        internal delegate void conf_set_dr_msg_cb_delegate(IntPtr p, DeliveryReportDelegate d);
        internal static conf_set_dr_msg_cb_delegate conf_set_dr_msg_cb;

        internal delegate void conf_set_rebalance_cb_delegate(IntPtr p, RebalanceDelegate d);
        internal static conf_set_rebalance_cb_delegate conf_set_rebalance_cb;

        internal delegate void conf_set_offset_commit_cb_delegate(IntPtr p, CommitDelegate d);
        internal static conf_set_offset_commit_cb_delegate conf_set_offset_commit_cb;

        internal delegate void conf_set_error_cb_delegate(IntPtr p, ErrorDelegate d);
        internal static conf_set_error_cb_delegate conf_set_error_cb;

        internal delegate void conf_set_log_cb_delegate(IntPtr p, LogDelegate d);
        internal static conf_set_log_cb_delegate conf_set_log_cb;

        internal delegate void conf_set_stats_cb_delegate(IntPtr p, StatsDelegate d);
        internal static conf_set_stats_cb_delegate conf_set_stats_cb;

        internal delegate void conf_set_default_topic_conf_delegate(IntPtr p, IntPtr p2);
        internal static conf_set_default_topic_conf_delegate conf_set_default_topic_conf;


        internal delegate ConfRes ConfGet(IntPtr conf, string name, StringBuilder dest, ref UIntPtr dest_size);
        internal static ConfGet conf_get;
        internal static ConfGet topic_conf_get;

        internal delegate IntPtr ConfDump(IntPtr conf, out UIntPtr cntp);
        internal static ConfDump conf_dump;
        internal static ConfDump topic_conf_dump;

        internal delegate void conf_dump_free_delegate(IntPtr p, UIntPtr p2);
        internal static conf_dump_free_delegate conf_dump_free;

        internal delegate SafeTopicConfigHandle topic_conf_new_delegate();
        internal static topic_conf_new_delegate topic_conf_new;

        internal delegate IntPtr topic_conf_dup_delegate(IntPtr p);
        internal static topic_conf_dup_delegate topic_conf_dup;

        internal delegate void topic_conf_destroy_delegate(IntPtr p);
        internal static topic_conf_destroy_delegate topic_conf_destroy;

        internal delegate ConfRes topic_conf_set_delegate(IntPtr p, string s, string s1, StringBuilder s2, UIntPtr p1);
        internal static topic_conf_set_delegate topic_conf_set;

        internal delegate void topic_conf_set_partitioner_cb_delegate(IntPtr p, PartitionerDelegate d);
        internal static topic_conf_set_partitioner_cb_delegate topic_conf_set_partitioner_cb;

        internal delegate bool topic_partition_available_delegate(IntPtr p, int i);
        internal static topic_partition_available_delegate topic_partition_available;

        internal delegate SafeKafkaHandle rd_new_delegate(RdKafkaType t, IntPtr p, StringBuilder s, UIntPtr p2);
        internal static rd_new_delegate rd_new;

        internal delegate void destroy_delegate(IntPtr p);
        internal static destroy_delegate destroy;

        internal delegate IntPtr name_delegate(IntPtr p);
        internal static name_delegate name;

        internal delegate IntPtr memberid_delegate(IntPtr p);
        internal static memberid_delegate memberid;

        internal delegate SafeTopicHandle topic_new_delegate(IntPtr p, string s, IntPtr p2);
        internal static topic_new_delegate topic_new;

        internal delegate void topic_destroy_delegate(IntPtr p);
        internal static topic_destroy_delegate topic_destroy;

        internal delegate IntPtr topic_name_delegate(IntPtr p);
        internal static topic_name_delegate topic_name;

        internal delegate ErrorCode poll_set_consumer_delegate(IntPtr p);
        internal static poll_set_consumer_delegate poll_set_consumer;

        internal delegate IntPtr poll_delegate(IntPtr p, IntPtr p1);
        internal static poll_delegate poll;

        internal delegate ErrorCode QueryOffsets(IntPtr rk, string topic, int partition,
                out long low, out long high, IntPtr timeout_ms);
        internal static QueryOffsets query_watermark_offsets;

        internal delegate ErrorCode GetOffsets(IntPtr rk, string topic, int partition,
                out long low, out long high);

        internal static GetOffsets get_watermark_offsets;

        internal delegate IntPtr mem_free_delegate(IntPtr p, IntPtr p2);
        internal static mem_free_delegate mem_free;

        internal delegate ErrorCode subscribe_delegate(IntPtr p, IntPtr p1);
        internal static subscribe_delegate subscribe;

        internal delegate ErrorCode unsubscribe_delegate(IntPtr p);
        internal static unsubscribe_delegate unsubscribe;
        
        internal delegate ErrorCode Subscription(IntPtr rk, out IntPtr topics);
        internal static Subscription subscription;

        internal delegate IntPtr consumer_poll_delegate(IntPtr p, IntPtr p2);
        internal static consumer_poll_delegate consumer_poll;

        internal delegate ErrorCode consumer_close_delegate(IntPtr p);
        internal static consumer_close_delegate consumer_close;

        internal delegate ErrorCode assign_delegate(IntPtr p, IntPtr p2);
        internal static assign_delegate assign;


        internal delegate ErrorCode Assignment(IntPtr rk, out IntPtr topics);
        internal static Assignment assignment;

        internal delegate ErrorCode commit_delegate(IntPtr p, IntPtr p2, bool b);
        internal static commit_delegate commit;

        internal delegate ErrorCode commit_queue_delegate(IntPtr p, IntPtr p2, IntPtr p3, CommitDelegate d, IntPtr p4);
        internal static commit_queue_delegate commit_queue;

        internal delegate ErrorCode committed_delegate(IntPtr p, IntPtr p1, IntPtr p2);
        internal static committed_delegate committed;

        internal delegate ErrorCode position_delegate(IntPtr p1, IntPtr p2);
        internal static position_delegate position;

        internal delegate IntPtr produce_delegate(IntPtr p, int i, IntPtr p2, IntPtr p3, UIntPtr p4, IntPtr p5, UIntPtr p6, IntPtr p7);
        internal static produce_delegate produce;

        internal delegate IntPtr producev_delegate(IntPtr p, int i, IntPtr p2, IntPtr p3, UIntPtr p4, IntPtr p5, UIntPtr p6, long l, IntPtr p7);
        internal static producev_delegate producev;


        internal delegate ErrorCode Flush(IntPtr rk, IntPtr timeout_ms);
        internal static Flush flush;

        internal delegate ErrorCode Metadata(IntPtr rk, bool all_topics,
                IntPtr only_rkt, out IntPtr metadatap, IntPtr timeout_ms);
        internal static Metadata metadata;

        internal delegate void metadata_destroy_delegate(IntPtr p);
        internal static metadata_destroy_delegate metadata_destroy;

        internal delegate ErrorCode ListGroups(IntPtr rk, string group,
                out IntPtr grplistp, IntPtr timeout_ms);
        internal static ListGroups list_groups;

        internal delegate void group_list_destroy_delegate(IntPtr p);
        internal static group_list_destroy_delegate group_list_destroy;

        internal delegate IntPtr brokers_add_delegate(IntPtr p, string s);
        internal static brokers_add_delegate brokers_add;

        internal delegate int outq_len_delegate(IntPtr p);
        internal static outq_len_delegate outq_len;

        //
        // Queues
        //
        internal delegate IntPtr queue_new_delegate(IntPtr p);
        internal static queue_new_delegate queue_new;

        internal delegate void queue_destroy_delegate(IntPtr p);
        internal static queue_destroy_delegate queue_destroy;

        internal delegate IntPtr queue_poll_delegate(IntPtr p, IntPtr p2);
        internal static queue_poll_delegate queue_poll;

        //
        // Events
        //

        internal delegate void event_destroy_delegate(IntPtr p);
        internal static event_destroy_delegate event_destroy;

        internal delegate int event_type_delegate(IntPtr p);
        internal static event_type_delegate event_type;

        internal delegate ErrorCode event_error_delegate(IntPtr p);
        internal static event_error_delegate event_error;

        internal delegate IntPtr event_error_string_delegate(IntPtr p);
        internal static event_error_string_delegate event_error_string;
        internal static string event_error_string_utf8(IntPtr rkev)
            => Util.Marshal.PtrToStringUTF8(event_error_string(rkev));

        internal delegate IntPtr event_topic_partition_list_delegate(IntPtr p);
        internal static event_topic_partition_list_delegate event_topic_partition_list;

    }
}
