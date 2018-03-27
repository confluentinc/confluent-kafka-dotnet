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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a message stored in Kafka.
    /// </summary>
    public class Message
    {
        /// <summary>
        ///     Gets the message value (possibly null).
        /// </summary>
        public byte[] Value { get; set; }

        /// <summary>
        ///     Gets the message key value (possibly null).
        /// </summary>
        public byte[] Key { get; set; }

        /// <summary>
        ///     The message timestamp. The timestamp type must be set to CreateTime. 
        ///     Specify Timestamp.Default to set the message timestamp to the time
        ///     of this function call.
        /// </summary>
        public Timestamp Timestamp { get; set; }

        /// <summary>
        ///     The collection of message headers (or null). Specifying null or an 
        ///      empty list are equivalent. The order of headers is maintained, and
        ///     duplicate header keys are allowed.
        /// </summary>
        public Headers Headers { get; set; }
    }
}
