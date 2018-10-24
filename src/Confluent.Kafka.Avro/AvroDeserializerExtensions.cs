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

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Avro.Generic;


namespace Confluent.Kafka.AvroSerdes
{
    /// <summary>
    ///     Avro deserialization related extension methods for
    ///     <see cref="Confluent.Kafka.Consumer" />
    /// </summary>
    public static class AvroDeserializerExtensions
    {
        /// <summary>
        ///     Poll for new messages / events. Blocks until a 
        ///     <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="consumer">
        ///     The <see cref="Confluent.Kafka.Consumer" /> instance to use to poll
        ///     for messages.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroDeserializer{T}" />
        ///     instance to use to deserialize message keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroDeserializer{T}" />
        ///     instance to use to deserialize message values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A <see cref="System.Threading.CancellationToken" /> that can be used
        ///     to abort this request.
        /// </param>
        /// <returns>
        ///     The <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />.
        /// </returns>
        public static async Task<ConsumeResult<TKey, TValue>> ConsumeAsync<TKey, TValue>(
            this Consumer consumer,
            AvroDeserializer<TKey> keyDeserializer,
            AvroDeserializer<TValue> valueDeserializer,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await Task.Run(() => consumer.Consume(cancellationToken));
            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Timestamp = result.Timestamp,
                    Headers = result.Headers,
                    Key = await keyDeserializer.Deserialize(result.Topic, result.Key, true),
                    Value = await valueDeserializer.Deserialize(result.Topic, result.Value, false)
                }
            };
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a 
        ///     <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="consumer">
        ///     The <see cref="Confluent.Kafka.Consumer" /> instance to use to poll
        ///     for messages.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The <see cref="Confluent.Kafka.Deserializer{T}" />
        ///     instance to use to deserialize message keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroDeserializer{T}" />
        ///     instance to use to deserialize message values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A <see cref="System.Threading.CancellationToken" /> that can be used
        ///     to abort this request.
        /// </param>
        /// <returns>
        ///     The <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />.
        /// </returns>
        public static async Task<ConsumeResult<TKey, TValue>> ConsumeAsync<TKey, TValue>(
            this Consumer consumer,
            Deserializer<TKey> keyDeserializer,
            AvroDeserializer<TValue> valueDeserializer,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await Task.Run(() => consumer.Consume(cancellationToken));
            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Timestamp = result.Timestamp,
                    Headers = result.Headers,
                    Key = keyDeserializer(result.Key, result.Key == null),
                    Value = await valueDeserializer.Deserialize(result.Topic, result.Value, false)
                }
            };
        }

        /// <summary>
        ///     Poll for new messages / events. Blocks until a 
        ///     <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="consumer">
        ///     The <see cref="Confluent.Kafka.Consumer" /> instance to use to poll
        ///     for messages.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroDeserializer{T}" />
        ///     instance to use to deserialize message keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The <see cref="Confluent.Kafka.Deserializer{T}" />
        ///     instance to use to deserialize message values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A <see cref="System.Threading.CancellationToken" /> that can be used
        ///     to abort this request.
        /// </param>
        /// <returns>
        ///     The <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />.
        /// </returns>
        public static async Task<ConsumeResult<TKey, TValue>> ConsumeAsync<TKey, TValue>(
            this Consumer consumer,
            AvroDeserializer<TKey> keyDeserializer,
            Deserializer<TValue> valueDeserializer,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await Task.Run(() => consumer.Consume(cancellationToken));
            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Timestamp = result.Timestamp,
                    Headers = result.Headers,
                    Key = await keyDeserializer.Deserialize(result.Topic, result.Key, true),
                    Value = valueDeserializer(result.Value, result.Value == null)
                }
            };
        }

    }
}
