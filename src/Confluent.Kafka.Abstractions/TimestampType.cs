// Copyright 2016-2017 Confluent Inc.
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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Enumerates the different meanings of a message timestamp value.
    /// </summary>
    public enum TimestampType
    {
        /// <summary>
        ///     Timestamp type is unknown.
        /// </summary>
        NotAvailable = 0,

        /// <summary>
        ///     Timestamp relates to message creation time as set by a Kafka client.
        /// </summary>
        CreateTime = 1,

        /// <summary>
        ///     Timestamp relates to the time a message was appended to a Kafka log.
        /// </summary>
        LogAppendTime = 2
    }
}
