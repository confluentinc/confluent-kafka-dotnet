// Copyright 2016-2018 Confluent Inc., 2015-2016 Andreas Heider
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a (deserialized) Kafka message.
    /// </summary>
    public class Message<TKey, TValue> : MessageMetadata
    {
        /// <summary>
        ///     Gets the message key value (possibly null).
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        ///     Gets the message value (possibly null).
        /// </summary>
        public TValue Value { get; set; }
    }

    /// <summary>
    ///     Represents a Kafka message.
    /// </summary>
    public class Message : Message<byte[], byte[]>
    {
        /// <summary>
        ///     Create a new instance with default property values.
        /// </summary>
        public Message() {}

        /// <summary>
        ///     Create a new instance that exactly mirrors the state of
        ///     a <see cref="Message{TKey, TValue}" /> instance.
        /// </summary>
        /// <param name="message">
        ///     The <see cref="Message{TKey, TValue}" /> instance to copy.
        /// </param>
        public Message(Message<byte[], byte[]> message)
        {
            Timestamp = message.Timestamp;
            Headers = message.Headers;
            Key = message.Key;
            Value = message.Value;
        }
    }
}
