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
using Confluent.Kafka.AvroClients;
using Avro.Generic;


namespace Confluent.Kafka.AvroClients
{
    /// <summary>
    ///     Extends <see cref="Confluent.Kafka.Producer" /> with functionality
    ///     for managing <see cref="Confluent.Kafka.AvroClients.AvroSerializer{T}" />
    ///     instances.
    /// </summary>
    public class AvroProducer : Producer
    {
        private Dictionary<Type, object> avroSerializers
            = new Dictionary<Type, object>();

        /// <summary>
        ///     Creates a new AvroProducer instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters.
        /// </param>
        public AvroProducer(IEnumerable<KeyValuePair<string, string>> config)
            : base(config) {}

        /// <summary>
        ///     Sets the Avro serializer that will be used to serialize keys or values with
        ///     the specified type.
        /// </summary>
        /// <param name="serializer">
        ///     The avro serializer.
        /// </param>
        public void RegisterAvroSerializer<T>(AvroSerializer<T> serializer)
        {
            avroSerializers.Add(typeof(T), serializer);
        }

        /// <summary>
        ///     Removes the avro serializer associated with the specified type.
        /// </summary>
        public void UnregisterAvroSerializer<T>()
        {
            avroSerializers.Remove(typeof(T));
        }

        /// <summary>
        ///     Gets the avro serializer that will be used to serialize values of the specified type.
        /// </summary>
        /// <returns>
        ///     The avro serializer corresponding to the specified type.
        /// </returns>
        public AvroSerializer<T> GetAvroSerializer<T>()
        {
            return (AvroSerializer<T>)avroSerializers[typeof(T)];
        }

        private async Task<Message> CreateMessage<TKey, TValue>(
            Message<TKey, TValue> message, string topic, SerdeType keySerdeType, SerdeType valueSerdeType)
        {
            try
            {
                byte[] keyBytes = null;
                if (keySerdeType == SerdeType.Regular)
                {
                    keyBytes = GetSerializer<TKey>()(message.Key);
                }
                else if (keySerdeType == SerdeType.Avro)
                {
                    keyBytes = await GetAvroSerializer<TKey>().Serialize(topic, message.Key, true);
                }

                byte[] valueBytes = null;
                if (valueSerdeType == SerdeType.Regular)
                {
                    valueBytes = GetSerializer<TValue>()(message.Value);
                }
                else if (valueSerdeType == SerdeType.Avro)
                {
                    valueBytes = await GetAvroSerializer<TValue>().Serialize(topic, message.Value, false);
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
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
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
        public async Task<DeliveryReport<TKey, TValue>> ProduceAsync<TKey, TValue>(
            string topic,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var dr = await ProduceAsync(
                    new TopicPartition(topic, Partition.Any),
                    await CreateMessage(message, topic, keySerdeType, valueSerdeType),
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
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
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
        public async Task<DeliveryReport<TKey, TValue>> ProduceAsync<TKey, TValue>(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                var dr = await ProduceAsync(
                    topicPartition,
                    await CreateMessage(message, topicPartition.Topic, keySerdeType, valueSerdeType),
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
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
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
        public async Task BeginProduceAsync<TKey, TValue>(
            string topic,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null)
        {
            try
            {
                BeginProduce(
                    new TopicPartition(topic, Partition.Any),
                    await CreateMessage(message, topic, keySerdeType, valueSerdeType),
                    (result) => deliveryHandler(
                        new DeliveryReportResult<TKey, TValue>
                        {
                            TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                            Message = message
                        }));
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        /// </summary>
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
        public async Task BeginProduceAsync<TKey, TValue>(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null)
        {
            try
            {
                BeginProduce(
                    topicPartition,
                    await CreateMessage(message, topicPartition.Topic, keySerdeType, valueSerdeType),
                    (result) => deliveryHandler(
                        new DeliveryReportResult<TKey, TValue>
                        {
                            TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                            Message = message
                        }));
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

    }
}
