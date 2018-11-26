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
    ///     Extends <see cref="Confluent.Kafka.Producer" /> with functionality
    ///     for managing <see cref="Confluent.Kafka.AvroSerdes.AvroSerializer{T}" />
    ///     instances.
    /// </summary>
    public class AvroProducer : Producer
    {
        private ISchemaRegistryClient schemaRegistryClient;

        private Dictionary<Type, object> avroSerializers
            = new Dictionary<Type, object>();

        /// <summary>
        ///     Creates a new AvroProducer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters.
        /// </param>
        public AvroProducer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config)
            : base(config)
        {
            this.schemaRegistryClient = schemaRegistryClient;
        }

        /// <summary>
        ///     Sets the Avro serializer that will be used to serialize keys or values with
        ///     the specified type.
        /// </summary>
        /// <param name="serializer">
        ///     The Avro serializer.
        /// </param>
        public void RegisterAvroSerializer<T>(AvroSerializer<T> serializer)
        {
            avroSerializers.Add(typeof(T), serializer);
        }

        /// <summary>
        ///     Removes the Avro serializer associated with the specified type.
        /// </summary>
        public void UnregisterAvroSerializer<T>()
        {
            avroSerializers.Remove(typeof(T));
        }

        /// <summary>
        ///     Gets the Avro serializer that will be used to serialize values of the specified type.
        /// </summary>
        /// <returns>
        ///     The Avro serializer corresponding to the specified type.
        /// </returns>
        public AvroSerializer<T> GetAvroSerializer<T>()
        {
            return (AvroSerializer<T>)avroSerializers[typeof(T)];
        }

        private AvroSerializer<T> GetOrCreateAvroSerializer<T>()
        {
            if (!avroSerializers.ContainsKey(typeof(T)))
            {
                avroSerializers.Add(typeof(T), new AvroSerializer<T>());
            }

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
                    keyBytes = await GetOrCreateAvroSerializer<TKey>().Serialize(schemaRegistryClient, topic, message.Key, true);
                }

                byte[] valueBytes = null;
                if (valueSerdeType == SerdeType.Regular)
                {
                    valueBytes = GetSerializer<TValue>()(message.Value);
                }
                else if (valueSerdeType == SerdeType.Avro)
                {
                    valueBytes = await GetOrCreateAvroSerializer<TValue>().Serialize(schemaRegistryClient, topic, message.Value, false);
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
        ///     The type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The type of serializer to use to serialize values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to abort this request.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> which will complete with 
        ///     a delivery report corresponding to the produce request, or an exception
        ///     if an error occured.
        /// </returns>
        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync<TKey, TValue>(
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

                return new DeliveryResult<TKey, TValue>
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
        ///     The type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The type of serializer to use to serialize values.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to abort this request.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> which will complete with 
        ///     a delivery report corresponding to the produce request, or an exception
        ///     if an error occured.
        /// </returns>
        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync<TKey, TValue>(
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

                return new DeliveryResult<TKey, TValue>
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
        ///     Note: The delivery handler callback will be called on an arbitrary thread,
        ///     c.f. the <see cref="Confluent.Kafka.Producer.BeginProduce(TopicPartition, Message, Action{DeliveryReport})" />
        ///     methods on <see cref="Confluent.Kafka.Producer" /> which will always be
        ///     called on the producer background thread.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     The type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The type of serializer to use to serialize values.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        public void BeginProduce<TKey, TValue>(
            string topic,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            // TODO: It would be better if this behaved in the same way as the
            // base BeginProduce method, but leaving as a todo (not a breaking change).
            Task.Run(() =>
            {
                BeginProduce(
                    new TopicPartition(topic, Partition.Any),
                    CreateMessage(message, topic, keySerdeType, valueSerdeType)
                        .ConfigureAwait(continueOnCapturedContext: false).GetAwaiter().GetResult(),
                    (result) => deliveryHandler(
                        new DeliveryReport<TKey, TValue>
                        {
                            TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                            Message = message
                        }));
            });
        }


        /// <summary>
        ///     Asynchronously produce a single
        ///     <see cref="Confluent.Kafka.Message{TKey, TValue}" /> to a Kafka topic.
        ///     Note: The delivery handler callback will be called on an arbitrary thread,
        ///     c.f. the <see cref="Confluent.Kafka.Producer.BeginProduce(TopicPartition, Message, Action{DeliveryReport})" />
        ///     methods on <see cref="Confluent.Kafka.Producer" /> which will always be
        ///     called on the producer background thread.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic/partition to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="keySerdeType">
        ///     The type of serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerdeType">
        ///     The type of serializer to use to serialize values.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        public void BeginProduce<TKey, TValue>(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            SerdeType keySerdeType,
            SerdeType valueSerdeType,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            // TODO: this will cause the delivery handler to be called on an arbitrary thread,
            // c.f. the BeginProduce method on `Producer` which will always be called on the 
            // background thread. It would be better if this behaved in the same way as the
            // base BeginProduce method, but leaving as a todo (not a breaking change).
            Task.Run(() => {
                BeginProduce(
                    topicPartition,
                    CreateMessage(message, topicPartition.Topic, keySerdeType, valueSerdeType)
                        .ConfigureAwait(continueOnCapturedContext: false).GetAwaiter().GetResult(),
                    (result) => deliveryHandler(
                        new DeliveryReport<TKey, TValue>
                        {
                            TopicPartitionOffsetError = result.TopicPartitionOffsetError,
                            Message = message
                        }));
            });
        }

    }
}
