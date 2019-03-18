// Copyright 2019 Confluent Inc.
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

using Confluent.Kafka.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Test helper methods.
    /// </summary>
    public static class Test
    {
        /// <summary>
        ///     Causes a fabricated fatal error in
        ///     the librdkafka client instance managed
        ///     by <paramref name="handle" />.
        /// </summary>
        /// <param name="handle">
        ///     A handle for a librdkafka client instance.
        /// </param>
        /// <param name="code">
        ///     The error code associated
        ///     with the fatal error.
        /// </param>
        /// <param name="reason">
        ///     The reason associated with
        ///     the fatal error.
        /// </param>
        /// <exception cref="KafkaException">
        ///     If a fatal error has
        ///     already occured on the provided librdkafka
        ///     instance, a KafkaException will be thrown
        ///     with the Error property set to
        ///     Local_PrevInProgress.
        /// </exception>
        public static void CauseFatalError(Handle handle, ErrorCode code, string reason)
        {
            handle.LibrdkafkaHandle.TestFatalError(code, reason);
        }
    }
}
