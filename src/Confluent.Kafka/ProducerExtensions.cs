// Copyright 2026 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Extension methods for <see cref="IProducer{TKey, TValue}"/>
    ///     providing a zero-allocation produce path using
    ///     <see cref="ReadOnlySpan{T}"/> of bytes.
    ///
    ///     Since librdkafka copies the payload synchronously
    ///     (MSG_F_COPY), the spans are safe to reuse immediately
    ///     after the method returns.
    /// </summary>
    public static class ProducerExtensions
    {
        /// <summary>
        ///     Produce a pre-serialized message to a Kafka topic
        ///     with zero managed heap allocation on the hot path.
        ///     The partition is determined by the partitioner
        ///     defined using the 'partitioner' configuration property.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="key">
        ///     The pre-serialized message key, or an empty span
        ///     for a null key.
        /// </param>
        /// <param name="value">
        ///     The pre-serialized message value, or an empty span
        ///     for a null value (tombstone).
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery
        ///     report corresponding to the produce request
        ///     (if enabled). Key and Value in the report will
        ///     always be null.
        /// </param>
        public static void Produce<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            string topic,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Action<DeliveryReport<Null, Null>> deliveryHandler = null)
        {
            if (producer is Producer<TKey, TValue> p)
            {
                p.Produce(topic, key, value, deliveryHandler);
            }
            else
            {
                throw new NotSupportedException(
                    "This producer instance does not support span-based produce. " +
                    "Use a producer created via ProducerBuilder.");
            }
        }

        /// <summary>
        ///     Produce a pre-serialized message to a Kafka
        ///     topic/partition with zero managed heap allocation
        ///     on the hot path.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="key">
        ///     The pre-serialized message key, or an empty span
        ///     for a null key.
        /// </param>
        /// <param name="value">
        ///     The pre-serialized message value, or an empty span
        ///     for a null value (tombstone).
        /// </param>
        /// <param name="timestamp">
        ///     The message timestamp. Use <see cref="Timestamp.Default"/>
        ///     for broker-assigned timestamp.
        /// </param>
        /// <param name="headers">
        ///     The message headers, or null.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery
        ///     report corresponding to the produce request
        ///     (if enabled). Key and Value in the report will
        ///     always be null.
        /// </param>
        public static void Produce<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value,
            Timestamp timestamp,
            Headers headers,
            Action<DeliveryReport<Null, Null>> deliveryHandler = null)
        {
            if (producer is Producer<TKey, TValue> p)
            {
                p.Produce(topicPartition, key, value, timestamp, headers, deliveryHandler);
            }
            else
            {
                throw new NotSupportedException(
                    "This producer instance does not support span-based produce. " +
                    "Use a producer created via ProducerBuilder.");
            }
        }
    }
}
