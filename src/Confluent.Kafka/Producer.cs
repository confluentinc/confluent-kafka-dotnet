// Copyright 2016-2018 Confluent Inc.
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A high level producer with serialization capability.
    /// </summary>
    public class Producer<TKey, TValue> : ProducerBase
    {
        private Serializer<TKey> keySerializer;
        private Serializer<TValue> valueSerializer;
        private ITaskSerializer<TKey> taskKeySerializer;
        private ITaskSerializer<TValue> taskValueSerializer;

        private Dictionary<Type, object> defaultSerializers = new Dictionary<Type, object>
        {
            { typeof(Null), Serializers.Null },
            { typeof(int), Serializers.Int32 },
            { typeof(long), Serializers.Long },
            { typeof(string), Serializers.UTF8 },
            { typeof(float), Serializers.Float },
            { typeof(double), Serializers.Double },
            { typeof(byte[]), Serializers.ByteArray }
        };


        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Producer" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.
        /// </param>
        /// <param name="keySerializer">
        ///     The serializer to use to serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     The serializer to use to serialize values.
        /// </param>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            Serializer<TKey> keySerializer = null,
            Serializer<TValue> valueSerializer = null
        ) : base(config)
        {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (this.keySerializer == null)
            {
                if (!defaultSerializers.TryGetValue(typeof(TKey), out object serializer))
                {
                    throw new ArgumentNullException(
                        $"Key serializer not specified and there is no default serializer defined for type {typeof(TKey)}");
                }
                this.keySerializer = (Serializer<TKey>)serializer;
            }

            if (this.valueSerializer == null)
            {
                if (!defaultSerializers.TryGetValue(typeof(TValue), out object serializer))
                {
                    throw new ArgumentNullException(
                        $"Value serializer not specified and there is no default serializer defined for type {typeof(TValue)}");
                }
                this.valueSerializer = (Serializer<TValue>)serializer;
            }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}" />.
        /// </summary>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            Serializer<TKey> keySerializer,
            ITaskSerializer<TValue> taskValueSerializer
        ) : base(config)
        {
            this.keySerializer = keySerializer;
            this.taskValueSerializer = taskValueSerializer;

            if (this.keySerializer == null)
            {
                throw new ArgumentNullException("Key serializer must be specified.");
            }

            if (this.taskValueSerializer == null)
            {
                throw new ArgumentNullException("Value serializer must be specified.");
            }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}" />.
        /// </summary>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            ITaskSerializer<TKey> taskKeySerializer,
            Serializer<TValue> valueSerializer
        ) : base(config)
        {
            this.taskKeySerializer = taskKeySerializer;
            this.valueSerializer = valueSerializer;

            if (this.taskKeySerializer == null)
            {
                throw new ArgumentNullException("Key serializer must be specified.");
            }

            if (this.valueSerializer == null)
            {
                throw new ArgumentNullException("Value serializer must be specified.");
            }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}" />.
        /// </summary>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            ITaskSerializer<TKey> taskKeySerializer,
            ITaskSerializer<TValue> taskValueSerializer
        ) : base(config)
        {
            this.taskKeySerializer = taskKeySerializer;
            this.taskValueSerializer = taskValueSerializer;

            if (this.taskKeySerializer == null)
            {
                throw new ArgumentNullException("Key serializer must be specified.");
            }

            if (this.valueSerializer == null)
            {
                throw new ArgumentNullException("Value serializer must be specified.");
            }
        }

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
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
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var keyBytes = (keySerializer != null)
                ? keySerializer(message.Key)
                : taskKeySerializer.Serialize(message.Key, true, topicPartition.Topic, message.Headers)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            var valBytes = (valueSerializer != null)
                ? valueSerializer(message.Value)
                : taskValueSerializer.Serialize(message.Value, false, topicPartition.Topic, message.Headers)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            if (this.enableDeliveryReports)
            {
                var handler = new TypedTaskDeliveryHandlerShim<TKey, TValue>(topicPartition.Topic,
                    enableDeliveryReportKey ? message.Key : default(TKey),
                    enableDeliveryReportValue ? message.Value : default(TValue));

                cancellationToken.Register(() => handler.TrySetException(new TaskCanceledException()));

                base.Produce(
                    topicPartition.Topic,
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                    message.Timestamp, topicPartition.Partition, message.Headers,
                    handler);

                return handler.Task;
            }
            else
            {
                base.Produce(
                    topicPartition.Topic, 
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, message.Headers, 
                    null);

                var result = new DeliveryResult<TKey, TValue>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Invalid),
                    Message = message
                };

                return Task.FromResult(result);
            }
        }

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined by
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
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
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => ProduceAsync(new TopicPartition(topic, Partition.Any), message, cancellationToken);

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined by
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
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
        public void BeginProduce(
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null
        )
            => BeginProduce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);


        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
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
        public void BeginProduce(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            var keyBytes = (keySerializer != null)
                ? keySerializer(message.Key)
                : taskKeySerializer.Serialize(message.Key, true, topicPartition.Topic, message.Headers)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            var valBytes = (valueSerializer != null)
                ? valueSerializer(message.Value)
                : taskValueSerializer.Serialize(message.Value, false, topicPartition.Topic, message.Headers)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            base.Produce(
                topicPartition.Topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, 
                message.Headers, 
                new TypedDeliveryHandlerShim_Action<TKey, TValue>(
                    topicPartition.Topic,
                    enableDeliveryReportKey ? message.Key : default(TKey),
                    enableDeliveryReportValue ? message.Value : default(TValue),
                    deliveryHandler)
            );
        }

        private class TypedTaskDeliveryHandlerShim<K, V> : TaskCompletionSource<DeliveryResult<K, V>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(string topic, K key, V val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
                Key = key;
                Value = val;
            }

            public string Topic;

            public K Key;

            public V Value;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
#if NET45
                    System.Threading.Tasks.Task.Run(() => TrySetResult(null));
#else
                    TrySetResult(null);
#endif
                    return;
                }

                var dr = new DeliveryResult<K, V>
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Message = new Message<K, V>
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

#if NET45
                if (deliveryReport.Error.IsError)
                {
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceException<K, V>(deliveryReport.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => TrySetResult(dr));
                }
#else
                if (deliveryReport.Error.IsError)
                {
                    TrySetException(new ProduceException<K, V>(deliveryReport.Error, dr));
                }
                else
                {
                    TrySetResult(dr);
                }
#endif
            }
        }

        private class TypedDeliveryHandlerShim_Action<K, V> : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(string topic, K key, V val, Action<DeliveryReport<K, V>> handler)
            {
                Topic = topic;
                Key = key;
                Value = val;
                Handler = handler;
            }

            public string Topic;

            public K Key;

            public V Value;

            public Action<DeliveryReport<K, V>> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReport<K, V>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message<K, V> 
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message == null 
                            ? new Timestamp(0, TimestampType.NotAvailable) 
                            : deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message?.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

                if (Handler != null)
                {
                    Handler(dr);
                }
            }
        }
    }


    /// <summary>
    ///     A high level producer.
    /// </summary>
    public class Producer : ProducerBase
    {
        /// <summary>
        ///     Creates a new Producer instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.
        /// </param>
        public Producer(IEnumerable<KeyValuePair<string, string>> config) : base(config) {}

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
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
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public Task<DeliveryResult> ProduceAsync(
            TopicPartition topicPartition,
            Message message,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this.enableDeliveryReports)
            {
                var handler = new TaskDeliveryHandlerShim(topicPartition.Topic,
                    enableDeliveryReportKey ? message.Key : null,
                    enableDeliveryReportValue ? message.Value : null);

                cancellationToken.Register(() => handler.TrySetException(new TaskCanceledException()));

                var keyBytes = message.Key;
                var valBytes = message.Value;
                
                base.Produce(
                    topicPartition.Topic, 
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, message.Headers, 
                    handler);

                return handler.Task;
            }
            else
            {
                var keyBytes = message.Key;
                var valBytes = message.Value;
                
                base.Produce(
                    topicPartition.Topic, 
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, message.Headers, 
                    null);

                var result = new DeliveryResult
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Invalid),
                    Message = message
                };

                return Task.FromResult(result);
            }
        }

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined using
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
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
        ///     A Task which will complete with a delivery report corresponding to
        ///     the produce request, or an exception if an error occured.
        /// </returns>
        public Task<DeliveryResult> ProduceAsync(
            string topic, Message message,
            CancellationToken cancellationToken = default(CancellationToken)
        )
            => ProduceAsync(new TopicPartition(topic, Partition.Any), message, cancellationToken);

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic/partition.
        /// </summary>
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
        public void BeginProduce(
            TopicPartition topicPartition,
            Message message,
            Action<DeliveryReport> deliveryHandler = null)
        {
            var keyBytes = message.Key;
            var valBytes = message.Value;

            base.Produce(
                topicPartition.Topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, 
                message.Headers, 
                new DeliveryHandlerShim_Action(
                    topicPartition.Topic,
                    enableDeliveryReportKey ? message.Key : null,
                    enableDeliveryReportValue ? message.Value : null,
                    deliveryHandler)
            );
        }

        /// <summary>
        ///     Asynchronously send a single message to a Kafka topic.
        ///     The partition the message is sent to is determined using
        ///     the partitioner defined using the 'partitioner' 
        ///     configuration property.
        /// </summary>
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
        public void BeginProduce(
            string topic, Message message,
            Action<DeliveryReport> deliveryHandler = null
        )
            => BeginProduce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);

        private class TaskDeliveryHandlerShim : TaskCompletionSource<DeliveryResult>, IDeliveryHandler
        {
            public TaskDeliveryHandlerShim(string topic, byte[] key, byte[] val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
                Key = key;
                Value = val;
            }

            public string Topic;

            public byte[] Key;

            public byte[] Value;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
#if NET45
                    System.Threading.Tasks.Task.Run(() => TrySetResult(null));
#else
                    TrySetResult(null);
#endif
                    return;
                }

                var dr = new DeliveryResult
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Message = new Message
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

#if NET45
                if (deliveryReport.Error.IsError)
                {
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceException(deliveryReport.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => TrySetResult(dr));
                }
#else
                if (deliveryReport.Error.IsError)
                {
                    TrySetException(new ProduceException(deliveryReport.Error, dr));
                }
                else
                {
                    TrySetResult(dr);
                }
#endif
            }
        }

        private class DeliveryHandlerShim_Action : IDeliveryHandler
        {
            public DeliveryHandlerShim_Action(string topic, byte[] key, byte[] val, Action<DeliveryReport> handler)
            {
                Topic = topic;
                Key = key;
                Value = val;
                Handler = handler;
            }

            public string Topic;

            public byte[] Key;

            public byte[] Value;

            public Action<DeliveryReport> Handler;

            public void HandleDeliveryReport(DeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReport
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message
                    {
                        Key = Key,
                        Value = Value,
                        Timestamp = deliveryReport.Message == null 
                            ? new Timestamp(0, TimestampType.NotAvailable) 
                            : deliveryReport.Message.Timestamp,
                        Headers = deliveryReport.Message?.Headers
                    }
                };
                // topic is cached in this object, not set in the deliveryReport to avoid the 
                // cost of marshalling it.
                dr.Topic = Topic;

                if (Handler != null)
                {
                    Handler(dr);
                }
            }
        }
    }
}