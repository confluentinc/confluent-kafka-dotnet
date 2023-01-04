// Copyright 2022 Confluent Inc.
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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Options for the DeleteGroups method.
    /// </summary>
    public class DeleteConsumerGroupOffsetsOptions
    {
        /// <summary>
        ///     The overall request timeout, including broker lookup, request 
        ///     transmission, operation time on broker, and response. If set
        ///     to null, the default request timeout for the AdminClient will
        ///     be used.
        /// 
        ///     Default: null
        /// </summary>
        public TimeSpan? RequestTimeout { get; set; }

        /// <summary>
        ///     The broker's operation timeout - the maximum time to wait for
        ///     DeleteConsumerGroupOffsetsAsync before returning a result to the application.
        ///     If set to null, will return immediately upon triggering record
        ///     deletion.
        /// 
        ///     Default: null
        /// </summary>
        public TimeSpan? OperationTimeout { get; set; }
    }
}
