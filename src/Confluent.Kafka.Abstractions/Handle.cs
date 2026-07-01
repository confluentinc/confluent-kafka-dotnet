// Copyright 2018 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     A handle for a librdkafka client instance. Also encapsulates 
    ///     a reference to the IClient instance that owns this handle.
    /// </summary>
    public class Handle
    {
        /// <summary>
        ///     Gets a value indicating whether the encapsulated librdkafka handle is invalid.
        /// </summary>
        /// <value>
        ///     <b>true</b> if the encapsulated librdkafka handle is invalid; otherwise, <b>false</b>.
        /// </value>
        public bool IsInvalid => this.LibrdkafkaHandle == null || this.LibrdkafkaHandle.IsInvalid;

        internal IClient Owner { get; set; }

        internal SafeKafkaHandle LibrdkafkaHandle { get; set; }
    }
}
