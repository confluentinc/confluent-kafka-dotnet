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

using System;

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Provides extension methods on the <see cref="Confluent.Kafka.Message" /> class.
    /// </summary>
    public static class MessageExtensions
    {
        /// <summary>
        ///     Deserializes a Message instance's key and value and returns 
        ///     the corresponding typed Message instance.
        /// </summary>
        /// <param name="message">
        ///     The message instance for which to deserialize the key and value.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The deserializer to use to deserialize the key.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The deserializer to use to deserialize the value.
        /// </param>
        /// <returns>
        ///     A typed message instance corresponding to <paramref name="message" />.
        /// </returns>
        public static Message<TKey, TValue> Deserialize<TKey, TValue>(this Message message, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            TKey key;
            TValue val;

            try
            {
                key = keyDeserializer.Deserialize(message.Topic, message.Key);
            }
            catch (Exception ex)
            {
                throw new KafkaException(new Error(ErrorCode.Local_KeyDeserialization, ex.ToString()), ex);
            }

            try
            {
                val = valueDeserializer.Deserialize(message.Topic, message.Value);
            }
            catch (Exception ex)
            {
                throw new KafkaException(new Error(ErrorCode.Local_ValueDeserialization, ex.ToString()), ex);
            }

            return new Message<TKey, TValue> (
                message.Topic,
                message.Partition,
                message.Offset,
                key,
                val,
                message.Timestamp,
                message.Error
            );
        }
    }
}
