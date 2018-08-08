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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Impl;

[assembly:CLSCompliant(true)]


namespace Confluent.Kafka
{
    /// <summary>
    ///     Methods that relate to the native librdkafka library itself
    ///     (do not require a Producer or Consumer broker connection).
    /// </summary>
    public static class Library
    {
        /// <summary>
        ///     Gets the librdkafka version as an integer.
        ///
        ///     Interpreted as hex MM.mm.rr.xx:
        ///         - MM = Major
        ///         - mm = minor
        ///         - rr = revision
        ///         - xx = pre-release id (0xff is the final release)
        ///
        ///     E.g.: 0x000901ff = 0.9.1
        /// </summary>
        public static int Version
        {
            get
            {
                Librdkafka.Initialize(null);
                return (int) Librdkafka.version();
            }
        }

        /// <summary>
        ///     Gets the librdkafka version as string.
        /// </summary>
        public static string VersionString
        {
            get
            {
                Librdkafka.Initialize(null);
                return Util.Marshal.PtrToStringUTF8(Librdkafka.version_str());
            }
        }

        /// <summary>
        ///     Gets a list of the supported debug contexts.
        /// </summary>
        public static string[] DebugContexts
        {
            get
            {
                Librdkafka.Initialize(null);
                return Util.Marshal.PtrToStringUTF8(Librdkafka.get_debug_contexts()).Split(',');
            }
        }

        /// <summary>
        ///     true if librdkafka has been successfully loaded, false if not.
        /// </summary>
        public static bool IsLoaded
            => Librdkafka.IsInitialized;

        /// <summary>
        ///     Loads the native librdkafka library. Does nothing if the library is
        ///     already loaded.
        /// </summary>
        /// <returns>
        ///     true if librdkafka was loaded as a result of this call, false if the
        ///     library has already been loaded.
        /// </returns>
        /// <remarks>
        ///     You will not typically need to call this method - librdkafka is loaded
        ///     automatically on first use of a Producer or Consumer instance.
        /// </remarks>
        public static bool Load()
            => Load(null);

        /// <summary>
        ///     Loads the native librdkafka library from the specified path (note: the 
        ///     specified path needs to include the filename). Does nothing if the 
        ///     library is already loaded.
        /// </summary>
        /// <returns>
        ///     true if librdkafka was loaded as a result of this call, false if the
        ///     library has already been loaded.
        /// </returns>
        /// <remarks>
        ///     You will not typically need to call this method - librdkafka is loaded
        ///     automatically on first use of a Producer or Consumer instance.
        /// </remarks>
        public static bool Load(string path)
            => Librdkafka.Initialize(path);

        private static object instanceCountLockObj = new object();
        private static int kafkaHandleCreateCount = 0;
        private static int kafkaHandleDestroyCount = 0;

        internal static void IncrementKafkaHandleCreateCount() { lock (instanceCountLockObj) { kafkaHandleCreateCount += 1; } }
        internal static void IncrementKafkaHandleDestroyCount() { lock (instanceCountLockObj) { kafkaHandleDestroyCount += 1; } }

        /// <summary>
        ///     The total number librdkafka client instances that have been
        ///     created and not yet disposed.
        /// </summary>
        /// <remarks>
        ///     This count includes any librdkafka client instances for which
        ///     all references have been lost (the Producer / Consumer /
        ///     AdminClient classes to not provide finalizers). 
        /// </remarks>
        public static int HandleCount
        {
            get { lock(instanceCountLockObj) { return kafkaHandleCreateCount - kafkaHandleDestroyCount; } }
        }
    }
}
