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
        private static async Task<DeliveryReport<TKey, TValue>> ProduceHelperAsync<TKey, TValue>(
            Producer producer,
            byte[] keyBytes,
            byte[] valBytes,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken)
        {
            var dr = await producer.ProduceAsync(
                topicPartition,
                new Message
                {
                    Key = keyBytes,
                    Value = valBytes,
                    Timestamp = message.Timestamp,
                    Headers = message.Headers
                },
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
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                await keySerializer.Serialize(topic, message.Key, true),
                await valueSerializer.Serialize(topic, message.Value, false),
                new TopicPartition(topic, Partition.Any),
                message,
                cancellationToken);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            Serializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                keySerializer(message.Key),
                await valueSerializer.Serialize(topic, message.Value, false),
                new TopicPartition(topic, Partition.Any),
                message,
                cancellationToken);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            Serializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                await keySerializer.Serialize(topic, message.Key, true),
                valueSerializer(message.Value),
                new TopicPartition(topic, Partition.Any),
                message,
                cancellationToken);


        // --- 


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                await keySerializer.Serialize(topicPartition.Topic, message.Key, true),
                await valueSerializer.Serialize(topicPartition.Topic, message.Value, false),
                topicPartition,
                message,
                cancellationToken);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            Serializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                keySerializer(message.Key),
                await valueSerializer.Serialize(topicPartition.Topic, message.Value, false),
                topicPartition,
                message,
                cancellationToken);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            Serializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => await ProduceHelperAsync<TKey, TValue>(
                producer,
                await keySerializer.Serialize(topicPartition.Topic, message.Key, true),
                valueSerializer(message.Value),
                topicPartition,
                message,
                cancellationToken);


        // ---


        private static void BeginProduceHelperAsync<TKey, TValue>(
            Producer producer,
            byte[] keyBytes,
            byte[] valBytes,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler)
        {
            var msg = new Message
            {
                Timestamp = message.Timestamp,
                Headers = message.Headers,
                Key = keyBytes,
                Value = valBytes
            };

            producer.BeginProduce(
                topicPartition, msg,
                (result) => deliveryHandler(
                    new DeliveryReportResult<TKey, TValue>
                    {
                        TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                        Message = message
                    }));
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduceHelperAsync(
                producer,
                await keySerializer.Serialize(topic, message.Key, true),
                await valueSerializer.Serialize(topic, message.Value, true),
                new TopicPartition(topic, Partition.Any),
                message,
                deliveryHandler);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            Serializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduceHelperAsync(
                producer,
                keySerializer(message.Key),
                await valueSerializer.Serialize(topic, message.Value, true),
                new TopicPartition(topic, Partition.Any),
                message,
                deliveryHandler);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            Serializer<TValue> valueSerializer,
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduceHelperAsync(
                producer,
                await keySerializer.Serialize(topic, message.Key, true),
                valueSerializer(message.Value),
                new TopicPartition(topic, Partition.Any),
                message,
                deliveryHandler);


        // ---


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The <see cref="Confluent.Kafka.Producer" /> instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduceHelperAsync(
                producer,
                await keySerializer.Serialize(topicPartition.Topic, message.Key, true),
                await valueSerializer.Serialize(topicPartition.Topic, message.Value, true),
                topicPartition,
                message,
                deliveryHandler);


         /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            Serializer<TKey> keySerializer,
            AvroSerializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler
        )
            => BeginProduceHelperAsync(
                producer,
                keySerializer(message.Key),
                await valueSerializer.Serialize(topicPartition.Topic, message.Value, true),
                topicPartition,
                message,
                deliveryHandler);


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic/partition.
        /// </summary>
        /// <param name="producer">
        ///     The producer instance to use to produce the message.
        /// </param>
        /// <param name="keySerializer">
        ///     The <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
        ///     instance to use to serialize the message key.
        /// </param>
        /// <param name="valueSerializer">
        ///     The <see cref="Confluent.Kafka.Serializer{T}" />
        ///     instance to use to serialize the message value.
        /// </param>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
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
            AvroSerializer<TKey> keySerializer,
            Serializer<TValue> valueSerializer,
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler
        )
            => BeginProduceHelperAsync(
                producer,
                await keySerializer.Serialize(topicPartition.Topic, message.Key, true),
                valueSerializer(message.Value),
                topicPartition,
                message,
                deliveryHandler);
    }
}
