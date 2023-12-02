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

        /// <summary>
        /// Implicit conversion from value to <see cref="Message{TKey, TValue}"/>.
        /// This conversion may be useful in case when key is <see cref="Null"/>.
        /// </summary>
        /// <param name="message">Value.</param>
        public static implicit operator Message<TKey, TValue>(TValue message)
        {
            return new Message<TKey, TValue> { Value = message };
        }

        /// <summary>
        /// Implicit conversion from tuple to <see cref="Message{TKey, TValue}"/>.
        /// </summary>
        /// <param name="message">Tuple of value and headers.</param>
        public static implicit operator Message<TKey, TValue>((TValue, Headers) message)
        {
            return new Message<TKey, TValue> { Value = message.Item1, Headers = message.Item2 };
        }

        /// <summary>
        /// Implicit conversion from tuple to <see cref="Message{TKey, TValue}"/>.
        /// </summary>
        /// <param name="message">Tuple of value and timestamp.</param>
        public static implicit operator Message<TKey, TValue>((TValue, Timestamp) message)
        {
            return new Message<TKey, TValue> { Value = message.Item1, Timestamp = message.Item2 };
        }

        /// <summary>
        /// Implicit conversion from tuple to <see cref="Message{TKey, TValue}"/>.
        /// </summary>
        /// <param name="message">Tuple of key and value.</param>
        public static implicit operator Message<TKey, TValue>((TKey, TValue) message)
        {
            return new Message<TKey, TValue> { Key = message.Item1, Value = message.Item2 };
        }

        /// <summary>
        /// Implicit conversion from tuple to <see cref="Message{TKey, TValue}"/>..
        /// </summary>
        /// <param name="message">Tuple of key, value, timestamp and headers.</param>
        public static implicit operator Message<TKey, TValue>((TKey, TValue, Timestamp, Headers) message)
        {
            return new Message<TKey, TValue>
            {
                Key = message.Item1,
                Value = message.Item2,
                Timestamp = message.Item3,
                Headers = message.Item4
            };
        }
    }
}
