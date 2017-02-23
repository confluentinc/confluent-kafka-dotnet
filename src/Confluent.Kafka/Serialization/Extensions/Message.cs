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

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     Provides extension methods on the <see cref="Confluent.Kafka.Message" /> class.
    /// </summary>
    public static class MessageExtensions
    {
        public static Message<TKey, TValue> Deserialize<TKey, TValue>(this Message message, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
            => new Message<TKey, TValue> (
                message.Topic,
                message.Partition,
                message.Offset,
                keyDeserializer.Deserialize(message.Key),
                valueDeserializer.Deserialize(message.Value),
                message.Timestamp,
                message.Error
            );
    }
}
