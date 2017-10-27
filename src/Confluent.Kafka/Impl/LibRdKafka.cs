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
using System.IO;
using System.Text;
using System.Reflection;
using System.Runtime.InteropServices;
using Confluent.Kafka.Internal;


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

        private static class LinuxNative
        {
            [DllImport("libdl.so")]
            public static extern IntPtr dlopen(String fileName, int flags);

            [DllImport("libdl.so")]
            public static extern IntPtr dlerror();

            [DllImport("libdl.so")]
            public static extern IntPtr dlsym(IntPtr handle, String symbol);
        }

        private static class OsxNative
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

        public static string LibraryPath { get; private set; }

        static void LoadLibrdkafka(string userSpecifiedLibrdkafkaPath)
        {
            LibraryOpenDelegate openLib = null;
            SymbolLookupDelegate lookupSymbol = null;
            Tuple<string, string>[] nugetLibrdkafkaNames = new Tuple<string, string>[0];

#if NET45 || NET46
            openLib = (string name)
                     => WindowsNative.LoadLibraryEx(
                            name,
                            IntPtr.Zero,
                            WindowsNative.LoadLibraryFlags.LOAD_WITH_ALTERED_SEARCH_PATH);
            lookupSymbol = WindowsNative.GetProcAddress;

            if (IntPtr.Size == 8)
            {
                nugetLibrdkafkaNames = new Tuple<string, string>[]
                {
                        Tuple.Create("win-x64", "librdkafka.dll"),
                };
            }
            else
            {
                nugetLibrdkafkaNames = new Tuple<string, string>[]
                {
                        Tuple.Create("win-x86", "librdkafka.dll")
                };
            }
#else
            const int RTLD_NOW = 2;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                openLib = (string name) => OsxNative.dlopen(name, RTLD_NOW);
                lookupSymbol = OsxNative.dlsym;
                if (RuntimeInformation.OSArchitecture == Architecture.X64)
                {
                    nugetLibrdkafkaNames = new Tuple<string, string>[]
                        { Tuple.Create("osx-x64", "librdkafka.dylib") };
                }
            } 
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                openLib = (string name) => LinuxNative.dlopen(name, RTLD_NOW);
                lookupSymbol = LinuxNative.dlsym;
                if (RuntimeInformation.OSArchitecture == Architecture.X64)
                {
                    // TODO: more sophisticated discovery / ordering of this list.
                    nugetLibrdkafkaNames = new Tuple<string, string>[]
                    {
                        Tuple.Create("debian-x64", "librdkafka.so"),
                        Tuple.Create("rhel-x64", "librdkafka.so"),
                        // Tuple.Create("linux-x64", "librdkafka-ssl-1.0.2.so"),
                        // Tuple.Create("linux-x64", "librdkafka-ssl-1.0.0.so"),
                    };
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                openLib = (string name)
                     => WindowsNative.LoadLibraryEx(
                            name, 
                            IntPtr.Zero, 
                            WindowsNative.LoadLibraryFlags.LOAD_WITH_ALTERED_SEARCH_PATH);
                lookupSymbol = WindowsNative.GetProcAddress;
                
                if (RuntimeInformation.OSArchitecture == Architecture.X64)
                {
                    nugetLibrdkafkaNames = new Tuple<string, string>[]
                    {
                        Tuple.Create("win-x64", "librdkafka.dll"),
                    };
                }
                else if (RuntimeInformation.OSArchitecture == Architecture.X86)
                {
                    nugetLibrdkafkaNames = new Tuple<string, string>[]
                    {
                        Tuple.Create("win-x86", "librdkafka.dll")
                    };
                }
            }
            else
            {
                throw new Exception($"Unsupported platform: {RuntimeInformation.OSDescription}");
            }
#endif

            var pathsTried = new List<string>();

            IntPtr librdkafkaAddr = IntPtr.Zero;
            if (userSpecifiedLibrdkafkaPath != null)
            {
                librdkafkaAddr = openLib(userSpecifiedLibrdkafkaPath);
                pathsTried.Add(userSpecifiedLibrdkafkaPath);
            }
            else
            {
                foreach (var librdkafkaName in nugetLibrdkafkaNames)
                {
#if NET45 || NET46
                    var baseUri = new Uri(Assembly.GetExecutingAssembly().GetName().EscapedCodeBase);
                    var baseDirectory = Path.GetDirectoryName(baseUri.LocalPath);
#else
                    var baseDirectory = Path.GetDirectoryName(typeof(Error).GetTypeInfo().Assembly.Location);
#endif
                    var path = Path.Combine(baseDirectory, "librdkafka", librdkafkaName.Item1, librdkafkaName.Item2);
                    pathsTried.Add(path);
                    librdkafkaAddr = openLib(path);
                    if (librdkafkaAddr != IntPtr.Zero)
                    {
                        break;
                    }
                }
            }

            if (librdkafkaAddr == IntPtr.Zero)
            {
                throw new DllNotFoundException("Failed to load librdkafka native library: " + string.Join("\n", pathsTried.ToArray()));
            }

            LibraryPath = pathsTried[pathsTried.Count-1];

            InitializeDelegates(librdkafkaAddr, lookupSymbol);
        }

        static void InitializeDelegates(IntPtr librdkafkaAddr, SymbolLookupDelegate lookup)
        {
#if NET45 || NET46
            version = (version_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_version"), typeof(version_delegate));
            version_str = (version_str_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_version_str"), typeof(version_str_delegate));
            get_debug_contexts = (get_debug_contexts_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_get_debug_contexts"), typeof(get_debug_contexts_delegate));
            err2str = (err2str_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_err2str"), typeof(err2str_delegate));
            topic_partition_list_new = (topic_partition_list_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_new"),typeof(topic_partition_list_new_delegate));
            topic_partition_list_destroy = (topic_partition_list_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_destroy"), typeof(topic_partition_list_destroy_delegate));
            topic_partition_list_add = (topic_partition_list_add_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_partition_list_add"), typeof(topic_partition_list_add_delegate));
            last_error = (last_error_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_last_error"), typeof(last_error_delegate));
            message_timestamp = (messageTimestampDelegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_message_timestamp"), typeof(messageTimestampDelegate));
            message_destroy = (message_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_message_destroy"), typeof(message_destroy_delegate));
            conf_new = (conf_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_new"), typeof(conf_new_delegate));
            conf_destroy = (conf_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_destroy"), typeof(conf_destroy_delegate));
            conf_dup = (conf_dup_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_dup"), typeof(conf_dup_delegate));
            conf_set = (conf_set_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set"), typeof(conf_set_delegate));
            conf_set_dr_msg_cb = (conf_set_dr_msg_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_dr_msg_cb"), typeof(conf_set_dr_msg_cb_delegate));
            conf_set_rebalance_cb = (conf_set_rebalance_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_rebalance_cb"), typeof(conf_set_rebalance_cb_delegate));
            conf_set_offset_commit_cb = (conf_set_offset_commit_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_offset_commit_cb"), typeof(conf_set_offset_commit_cb_delegate));
            conf_set_error_cb = (conf_set_error_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_error_cb"), typeof(conf_set_error_cb_delegate));
            conf_set_log_cb = (conf_set_log_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_log_cb"), typeof(conf_set_log_cb_delegate));
            conf_set_stats_cb = (conf_set_stats_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_stats_cb"), typeof(conf_set_stats_cb_delegate));
            conf_set_default_topic_conf = (conf_set_default_topic_conf_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_set_default_topic_conf"), typeof(conf_set_default_topic_conf_delegate));
            conf_get = (ConfGet)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_get"), typeof(ConfGet));
            topic_conf_get = (ConfGet)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_get"), typeof(ConfGet));
            conf_dump = (ConfDump)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_dump"), typeof(ConfDump));
            topic_conf_dump = (ConfDump)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_dump"), typeof(ConfDump));
            conf_dump_free = (conf_dump_free_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_conf_dump_free"), typeof(conf_dump_free_delegate));
            topic_conf_new = (topic_conf_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_new"), typeof(topic_conf_new_delegate));
            topic_conf_dup = (topic_conf_dup_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_dup"), typeof(topic_conf_dup_delegate));
            topic_conf_destroy = (topic_conf_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_destroy"), typeof(topic_conf_destroy_delegate));
            topic_conf_set = (topic_conf_set_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_set"), typeof(topic_conf_set_delegate));
            topic_conf_set_partitioner_cb = (topic_conf_set_partitioner_cb_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_conf_set_partitioner_cb"), typeof(topic_conf_set_partitioner_cb_delegate));
            topic_partition_available = (topic_partition_available_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_partition_available"), typeof(topic_partition_available_delegate));
            kafka_new = (kafka_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_new"), typeof(kafka_new_delegate));
            destroy = (destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_destroy"), typeof(destroy_delegate));
            name = (name_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_name"), typeof(name_delegate));
            memberid = (memberid_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_memberid"), typeof(memberid_delegate));
            topic_new = (topic_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_new"), typeof(topic_new_delegate));
            topic_destroy = (topic_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_destroy"), typeof(topic_destroy_delegate));
            topic_name = (topic_name_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_topic_name"), typeof(topic_name_delegate));
            poll = (poll_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_poll"), typeof(poll_delegate));
            poll_set_consumer = (poll_set_consumer_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_poll_set_consumer"), typeof(poll_set_consumer_delegate));
            query_watermark_offsets = (QueryOffsets)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_query_watermark_offsets"), typeof(QueryOffsets));
            get_watermark_offsets = (GetOffsets)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_get_watermark_offsets"), typeof(GetOffsets));
            mem_free = (mem_free_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_mem_free"), typeof(mem_free_delegate));
            subscribe = (subscribe_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_subscribe"), typeof(subscribe_delegate));
            unsubscribe = (unsubscribe_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_unsubscribe"), typeof(unsubscribe_delegate));
            subscription = (Subscription)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_subscription"), typeof(Subscription));
            consumer_poll = (consumer_poll_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_consumer_poll"), typeof(consumer_poll_delegate));
            consumer_close = (consumer_close_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_consumer_close"), typeof(consumer_close_delegate));
            assign = (assign_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_assign"), typeof(assign_delegate));
            assignment = (Assignment)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_assignment"), typeof(Assignment));
            commit = (commit_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_commit"), typeof(commit_delegate));
            commit_queue = (commit_queue_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_commit_queue"), typeof(commit_queue_delegate));
            committed = (committed_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_committed"), typeof(committed_delegate));
            position = (position_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_position"), typeof(position_delegate));
            produce = (produce_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_produce"), typeof(produce_delegate));
            producev = (producev_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_producev"), typeof(producev_delegate));
            flush = (Flush)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_flush"), typeof(Flush));
            metadata = (Metadata)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_metadata"), typeof(Metadata));
            metadata_destroy = (metadata_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_metadata_destroy"), typeof(metadata_destroy_delegate));
            list_groups = (ListGroups)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_list_groups"), typeof(ListGroups));
            group_list_destroy = (group_list_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_group_list_destroy"), typeof(group_list_destroy_delegate));
            brokers_add = (brokers_add_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_brokers_add"), typeof(brokers_add_delegate));
            outq_len = (outq_len_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_outq_len"), typeof(outq_len_delegate));
            queue_new = (queue_new_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_queue_new"), typeof(queue_new_delegate));
            queue_destroy = (queue_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_queue_destroy"), typeof(queue_destroy_delegate));
            event_type = (event_type_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_event_type"), typeof(event_type_delegate));
            event_error = (event_error_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_event_error"), typeof(event_error_delegate));
            event_error_string = (event_error_string_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_event_error_string"), typeof(event_error_string_delegate));
            event_topic_partition_list = (event_topic_partition_list_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_event_topic_partition_list"), typeof(event_topic_partition_list_delegate));
            event_destroy = (event_destroy_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_event_destroy"), typeof(event_destroy_delegate));
            queue_poll = (queue_poll_delegate)Marshal.GetDelegateForFunctionPointer(lookup(librdkafkaAddr, "rd_kafka_queue_poll"), typeof(queue_poll_delegate));
#else
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
            kafka_new = Marshal.GetDelegateForFunctionPointer<kafka_new_delegate>(lookup(librdkafkaAddr, "rd_kafka_new"));
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
#endif
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

        public static void Initialize(string userSpecifiedLibrdkafkaPath)
        {
            lock (lockObj)
            {
                if (!isInitialized)
                {
                    LoadLibrdkafka(userSpecifiedLibrdkafkaPath);

                    if ((long)version() < minVersion)
                    {
                        throw new FileLoadException($"Invalid librdkafka version {(long)version():x}, expected at least {minVersion:x}");
                    }

                    isInitialized = true;
                }
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

        internal delegate SafeKafkaHandle kafka_new_delegate(RdKafkaType t, IntPtr p, StringBuilder s, UIntPtr p2);
        internal static kafka_new_delegate kafka_new;

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
