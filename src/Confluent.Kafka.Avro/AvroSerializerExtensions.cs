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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.SchemaRegistry;
using Confluent.Kafka.AvroSerdes;
using Avro.Generic;


namespace Confluent.Kafka.AvroSerdes
{
    /// <summary>
    ///     Avro serialization related extension methods for
    ///     <see cref="Confluent.Kafka.Producer" />
    /// </summary>
    public static class AvroSerializerExtensions
    {
        /// <summary>
        ///     Sets the Avro serializer that will be used to serialize keys or values with
        ///     the specified type.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance this applies to.
        /// </param>
        /// <param name="serializer">
        ///     The avro serializer.
        /// </param>
        public static void RegisterAvroSerializer<T>(this Producer producer, AvroSerializer<T> serializer)
        {
            if (!SerdeState.serializers.ContainsKey(producer))
            {
                SerdeState.serializers.Add(producer, new Dictionary<Type, object>());
            }

            SerdeState.serializers[producer].Add(typeof(T), serializer);
        }


        /// <summary>
        ///     Removes the avro serializer associated with the specified type.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance this applies to.
        /// </param>
        public static void UnregisterAvroSerializer<T>(this Producer producer)
        {
            SerdeState.serializers[producer].Remove(typeof(T));
        }


        /// <summary>
        ///     Gets the avro serializer that will be used to serialize values of the specified type.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance this applies to.
        /// </param>
        /// <returns>
        ///     The avro serializer corresponding to the specified type.
        /// </returns>
        public static AvroSerializer<T> GetAvroSerializer<T>(this Producer producer)
        {
            return (AvroSerializer<T>)SerdeState.serializers[producer][typeof(T)];
        }


        private static async Task<Message> CreateMessage<TKey, TValue>(
            Producer producer, Message<TKey, TValue> message, string topic, SerdeType keySerdeType, SerdeType valueSerdeType)
        {
            byte[] keyBytes = null;
            if (keySerdeType == SerdeType.Regular)
            {
                keyBytes = producer.GetSerializer<TKey>()(message.Key);
            }
            else if (keySerdeType == SerdeType.Avro)
            {
                keyBytes = await producer.GetAvroSerializer<TKey>().Serialize(topic, message.Key, true);
            }

            byte[] valueBytes = null;
            if (valueSerdeType == SerdeType.Regular)
            {
                valueBytes = producer.GetSerializer<TValue>()(message.Value);
            }
            else if (valueSerdeType == SerdeType.Avro)
            {
                valueBytes = await producer.GetAvroSerializer<TValue>().Serialize(topic, message.Value, false);
            }

            return 
                new Message
                {
                    Key = keyBytes,
                    Value = valueBytes,
                    Timestamp = message.Timestamp,
                    Headers = message.Headers
                };
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     Which type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     Which type of serializer to use to serialize values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to abort this request.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> which will complete with 
        ///     a delivery report corresponding to the produce request, or an exception
        ///     if an error occured.
        /// </returns>
        public static async Task<DeliveryReport<TKey, TValue>> ProduceAsync<TKey, TValue>(
            this Producer producer,
            string topic,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var dr = await producer.ProduceAsync(
                new TopicPartition(topic, Partition.Any),
                await CreateMessage(producer, message, topic, keySerdeType, valueSerdeType),
                cancellationToken
            );

            return new DeliveryReport<TKey, TValue>
            {
                TopicPartitionOffset = dr.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Key = message.Key,
                    Value = message.Value,
                    Timestamp = dr.Timestamp,
                    Headers = dr.Headers
                }
            };
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     Which type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     Which type of serializer to use to serialize values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to abort this request.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> which will complete with 
        ///     a delivery report corresponding to the produce request, or an exception
        ///     if an error occured.
        /// </returns>
        public static async Task<DeliveryReport<TKey, TValue>> ProduceAsync<TKey, TValue>(
            this Producer producer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var dr = await producer.ProduceAsync(
                topicPartition,
                await CreateMessage(producer, message, topicPartition.Topic, keySerdeType, valueSerdeType),
                cancellationToken
            );

            return new DeliveryReport<TKey, TValue>
            {
                TopicPartitionOffset = dr.TopicPartitionOffset,
                Message = new Message<TKey, TValue>
                {
                    Key = message.Key,
                    Value = message.Value,
                    Timestamp = dr.Timestamp,
                    Headers = dr.Headers
                }
            };
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     Which type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     Which type of serializer to use to serialize values.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        /// <returns>
        ///     A <see cref="Task" /> that completes after serialization is complete and
        ///     the produce call has been forwarded to librdkafka. 
        /// </returns>
        public static async Task BeginProduceAsync<TKey, TValue>(
            this Producer producer,
            string topic,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => producer.BeginProduce(
                new TopicPartition(topic, Partition.Any),
                await CreateMessage(producer, message, topic, keySerdeType, valueSerdeType),
                (result) => deliveryHandler(
                    new DeliveryReportResult<TKey, TValue>
                    {
                        TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                        Message = message
                    }));


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     Which type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     Which type of serializer to use to serialize values.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        /// <returns>
        ///     A <see cref="Task" /> that completes after serialization is complete and
        ///     the produce call has been forwarded to librdkafka. 
        /// </returns>
        public static async Task BeginProduceAsync<TKey, TValue>(
            this Producer producer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => producer.BeginProduce(
                topicPartition,
                await CreateMessage(producer, message, topicPartition.Topic, keySerdeType, valueSerdeType),
                (result) => deliveryHandler(
                    new DeliveryReportResult<TKey, TValue>
                    {
                        TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                        Message = message
                    }));

    }
}
