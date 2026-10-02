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
// Refer to LICENSE for more information.

using System;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    ///     An Error subtype that is initialized from a native rd_kafka_error_t pointer.
    ///     Reads fields from the native object and (optionally) destroys it.
    ///     Lives in Confluent.Kafka because it depends on Librdkafka (P/Invoke).
    /// </summary>
    internal sealed class NativeError : Error
    {
        /// <summary>
        ///     Initialize from a native rd_kafka_error_t pointer, then destroy it.
        /// </summary>
        internal NativeError(IntPtr error) : this(error, true) { }

        /// <summary>
        ///     Initialize from a native rd_kafka_error_t pointer,
        ///     destroying it afterward only if <paramref name="destroy"/> is true.
        /// </summary>
        internal NativeError(IntPtr error, bool destroy)
            : base(
                error == IntPtr.Zero ? ErrorCode.NoError : Librdkafka.error_code(error),
                error == IntPtr.Zero ? null : Librdkafka.error_string(error),
                error != IntPtr.Zero && Librdkafka.error_is_fatal(error),
                error != IntPtr.Zero && Librdkafka.error_is_retriable(error),
                error != IntPtr.Zero && Librdkafka.error_txn_requires_abort(error))
        {
            if (error != IntPtr.Zero && destroy)
            {
                Librdkafka.error_destroy(error);
            }
        }
    }

}
