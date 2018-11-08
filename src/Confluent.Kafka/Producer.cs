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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A serializer for use with <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
    /// </summary>
    /// <param name="topic">
    ///     The topic the data is being produced to.
    /// </param>
    /// <param name="data">
    ///     The value to serialize.
    /// </param>
    /// <returns>
    ///     The serialized value.
    /// </returns>
    public delegate byte[] Serializer<T>(string topic, T data);

    /// <summary>
    ///     A generator for <see cref="Confluent.Kafka.Serializer{T}" /> instances.
    /// </summary>
    /// <param name="forKey">
    ///     Whether or not the serializer is for use with keys.
    /// </param>
    /// <returns>
    ///     The serializer.
    /// </returns>
    public delegate Serializer<T> SerializerGenerator<T>(bool forKey);

    /// <summary>
    ///     Implements a high-level Apache Kafka producer with key
    ///     and value serialization.
    /// </summary>
    public class Producer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly Producer ownedClient;
        private readonly Handle handle;
        private Producer producer;
        private Serializer<TKey> keySerializer;
        private Serializer<TValue> valueSerializer;

        private void setAndValidateSerializers(Serializer<TKey> keySerializer, Serializer<TValue> valueSerializer)
        {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (keySerializer != null && (object)keySerializer == (object)valueSerializer)
            {
                throw new ArgumentException("Key and value serializers must not be the same object.");
            }

            if (keySerializer == null)
            {
                if (typeof(TKey) == typeof(Null))
                {
                    this.keySerializer = Serializers.Null as Serializer<TKey>;
                }
                else if (typeof(TKey) == typeof(byte[]))
                {
                    this.keySerializer = Serializers.ByteArray as Serializer<TKey>;
                }
                else if (typeof(TKey) == typeof(string))
                {
                    this.keySerializer = Serializers.UTF8 as Serializer<TKey>;
                }
                else 
                {
                    throw new ArgumentNullException("Key serializer must be specified.");
                }
            }

            if (valueSerializer == null)
            {
                if (typeof(TValue) == typeof(Null))
                {
                    this.valueSerializer = Serializers.Null as Serializer<TValue>;
                }
                else if (typeof(TValue) == typeof(byte[]))
                {
                    this.valueSerializer = Serializers.ByteArray as Serializer<TValue>;
                }
                else if (typeof(TValue) == typeof(string))
                {
                    this.valueSerializer = Serializers.UTF8 as Serializer<TValue>;
                }
                else
                {
                    throw new ArgumentNullException("Value serializer must be specified.");
                }
            }
        }


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
        /// <param name="keySerializer">
        ///     A delegate to use for serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     A delegate to use for serialize values.
        /// </param>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            Serializer<TKey> keySerializer = null,
            Serializer<TValue> valueSerializer = null
        ) : this(config, (forKey) => keySerializer, (forKey) => valueSerializer) {}


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
        /// <param name="keySerializerGenerator">
        ///     A delegate to use to create a delegate for serializing keys.
        /// </param>
        /// <param name="valueSerializerGenerator">
        ///     A delegate to use to create a delegate for serialize values.
        /// </param>
        public Producer(
            IEnumerable<KeyValuePair<string, string>> config,
            SerializerGenerator<TKey> keySerializerGenerator,
            SerializerGenerator<TValue> valueSerializerGenerator)
        {
            this.ownedClient = new Producer(config);

            this.handle = ownedClient.Handle;
            this.producer = ownedClient;
            setAndValidateSerializers(
                keySerializerGenerator == null ? null: keySerializerGenerator(true),
                valueSerializerGenerator == null ? null : valueSerializerGenerator(false));
        }


        /// <summary>
        ///     Creates a new Producer instance
        /// </summary>
        /// <param name="handle">
        ///     A librdkafka handle to use for Kafka cluster communication.
        /// </param>
        /// <param name="keySerializer">
        ///     A delegate to use for serialize keys.
        /// </param>
        /// <param name="valueSerializer">
        ///     A delegate to use for serialize values.
        /// </param>
        public Producer(
            Handle handle,
            Serializer<TKey> keySerializer = null,
            Serializer<TValue> valueSerializer = null)
        {
            if (!(handle.Owner is Producer))
            {
                throw new ArgumentException("Handle must be owned by another Producer instance");
            }

            this.ownedClient = null;
            this.handle = handle;
            this.producer = (Producer)handle.Owner;
            setAndValidateSerializers(keySerializer, valueSerializer);
        }


        /// <summary>
        ///     Creates a new Producer instance
        /// </summary>
        /// <param name="handle">
        ///     A librdkafka handle to use for Kafka cluster communication.
        /// </param>
        /// <param name="keySerializerGenerator">
        ///     A delegate to use to create a delegate for serializing keys.
        /// </param>
        /// <param name="valueSerializerGenerator">
        ///     A delegate to use to create a delegate for serialize values.
        /// </param>
        public Producer(
            Handle handle,
            SerializerGenerator<TKey> keySerializerGenerator,
            SerializerGenerator<TValue> valueSerializerGenerator)
        {
            if (!(handle.Owner is Producer))
            {
                throw new ArgumentException("Handle must be owned by another Producer instance");
            }

            this.ownedClient = null;
            this.handle = handle;
            this.producer = (Producer)handle.Owner;
            setAndValidateSerializers(
                keySerializerGenerator == null ? null : keySerializerGenerator(true),
                valueSerializerGenerator == null ? null : valueSerializerGenerator(false));
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.Name" />
        /// </summary>
        public string Name
            => this.handle.Owner.Name;


        internal int Flush(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Flush(millisecondsTimeout);


        /// <summary>
        ///     Wait until all outstanding produce requests and delivery report
        ///     callbacks are completed.
        ///    
        ///     [API-SUBJECT-TO-CHANGE] - the semantics and/or type of the return value
        ///     is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time to block. You should typically use a
        ///     relatively short timout period and loop until the return value
        ///     becomes zero because this operation cannot be cancelled. 
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. This should be interpreted
        ///     as a rough indication of the number of messages waiting to be sent
        ///     to or acknowledged by the broker. If zero, there are no outstanding
        ///     messages or callbacks. Specifically, the value is equal to the sum
        ///     of the number of produced messages for which a delivery report has
        ///     not yet been handled and a number which is less than or equal to the
        ///     number of pending delivery report callback events (as determined by
        ///     the number of outstanding protocol requests).
        /// </returns>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. The wait time is bounded by the
        ///     timeout parameter.
        ///    
        ///     A related configuration parameter is message.timeout.ms which determines
        ///     the maximum length of time librdkafka attempts to deliver a message 
        ///     before giving up and so also affects the maximum time a call to Flush 
        ///     may block.
        /// </remarks>
        public int Flush(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Flush(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Wait until all outstanding produce requests and delivery report
        ///     callbacks are completed.
        /// </summary>
        /// <remarks>
        ///     This method should typically be called prior to destroying a producer
        ///     instance to make sure all queued and in-flight produce requests are
        ///     completed before terminating. 
        ///    
        ///     A related configuration parameter is message.timeout.ms which determines
        ///     the maximum length of time librdkafka attempts to deliver a message 
        ///     before giving up and so also affects the maximum time a call to Flush 
        ///     may block.
        /// </remarks>
        /// <exception cref="System.OperationCanceledException">
        ///     Thrown if the operation is cancelled.
        /// </exception>
        public void Flush(CancellationToken cancellationToken)
        {
            while (true)
            {
                int result = Flush(100);
                if (result == 0)
                {
                    return;
                }
                if (cancellationToken.IsCancellationRequested)
                {
                    // TODO: include flush number in exception.
                    throw new OperationCanceledException();
                }
            }
        }


        internal int Poll(int millisecondsTimeout)
            => ((Producer)this.handle.Owner).Poll(millisecondsTimeout);


        /// <summary>
        ///     Poll for callback events. Typically, you should not 
        ///     call this method. Only call on producer instances 
        ///     where background polling has been disabled.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time to block if no callback events
        ///     are waiting. You should typically use a relatively short 
        ///     timout period because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served.
        /// </returns>
        public int Poll(TimeSpan timeout)
            => ((Producer)this.handle.Owner).Poll(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Releases all resources used by this Producer. In the current
        ///     implementation, this method may block for up to 100ms. This 
        ///     will be replaced with a non-blocking version in the future.
        /// </summary>
        /// <remarks>
        ///     You will often want to call <see cref="Flush(int)" />
        ///     before disposing a Producer instance to make sure all
        ///     outstanding/queued messages are delivered.
        /// </remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     Releases the unmanaged resources used by the
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        ///     and optionally disposes the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources;
        ///     false to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (ownedClient == this.handle.Owner) 
                {
                    ownedClient.Dispose();
                }
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnLog" />.
        /// </summary>
        public event EventHandler<LogMessage> OnLog
        {
            add { handle.Owner.OnLog += value; }
            remove { handle.Owner.OnLog -= value; }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnStatistics" />.
        /// </summary>
        public event EventHandler<string> OnStatistics
        {
            add { handle.Owner.OnStatistics += value; }
            remove { handle.Owner.OnStatistics -= value; }
        }

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnError" />.
        /// </summary>
        public event EventHandler<ErrorEvent> OnError
        {
            add { handle.Owner.OnError += value; }
            remove { handle.Owner.OnError -= value; }
        }
        
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.AddBrokers(string)" />
        /// </summary>
        public int AddBrokers(string brokers)
            => this.handle.Owner.AddBrokers(brokers);


        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        ///     This can be used to construct an <see cref="Confluent.Kafka.AdminClient" />
        ///     or <see cref="Confluent.Kafka.Producer{TKey, TValue}" /> instance that
        ///     utilizes the same underlying librdkafka client as this Producer 
        ///     instance.
        /// </summary>
        public Handle Handle 
            => handle;


        private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<DeliveryReport<TKey, TValue>>, IDeliveryHandler
        {
            public TypedTaskDeliveryHandlerShim(string topic, TKey key, TValue val)
#if !NET45
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
#endif
            {
                Topic = topic;
                Key = key;
                Value = val;
            }

            public string Topic;

            public TKey Key;

            public TValue Value;

            public void HandleDeliveryReport(Producer.UntypedDeliveryReport deliveryReport)
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

                var dr = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Message = new Message<TKey, TValue>
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
                    System.Threading.Tasks.Task.Run(() => SetException(new ProduceException<TKey, TValue>(deliveryReport.Error, dr)));
                }
                else
                {
                    System.Threading.Tasks.Task.Run(() => TrySetResult(dr));
                }
#else
                if (deliveryReport.Error.IsError)
                {
                    TrySetException(new ProduceException<TKey, TValue>(deliveryReport.Error, dr));
                }
                else
                {
                    TrySetResult(dr);
                }
#endif
            }
        }


        private class TypedDeliveryHandlerShim_Action : IDeliveryHandler
        {
            public TypedDeliveryHandlerShim_Action(string topic, TKey key, TValue val, Action<DeliveryReportResult<TKey, TValue>> handler)
            {
                Topic = topic;
                Key = key;
                Value = val;
                Handler = handler;
            }

            public string Topic;

            public TKey Key;

            public TValue Value;

            public Action<DeliveryReportResult<TKey, TValue>> Handler;

            public void HandleDeliveryReport(Producer.UntypedDeliveryReport deliveryReport)
            {
                if (deliveryReport == null)
                {
                    return;
                }

                var dr = new DeliveryReportResult<TKey, TValue>
                {
                    TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                    Message = new Message<TKey, TValue> 
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
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this.producer.enableDeliveryReports)
            {
                var handler = new TypedTaskDeliveryHandlerShim(topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey),
                    producer.enableDeliveryReportValue ? message.Value : default(TValue));

                using (cancellationToken.Register(() => handler.TrySetException(new TaskCanceledException())))
                {
                    var keyBytes = keySerializer(topic, message.Key);
                    var valBytes = valueSerializer(topic, message.Value);

                    producer.ProduceImpl(
                        topic,
                        valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                        keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                        message.Timestamp, Partition.Any, message.Headers,
                        handler);

                    return handler.Task;
                }
            }
            else
            {
                var keyBytes = keySerializer(topic, message.Key);
                var valBytes = valueSerializer(topic, message.Value);

                producer.ProduceImpl(
                    topic,
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                    message.Timestamp, Partition.Any, message.Headers, 
                    null);

                var result = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, Partition.Any), Offset.Invalid),
                    Message = message
                };

                return Task.FromResult(result);
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
        public Task<DeliveryReport<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this.producer.enableDeliveryReports)
            {
                var handler = new TypedTaskDeliveryHandlerShim(topicPartition.Topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey),
                    producer.enableDeliveryReportValue ? message.Value : default(TValue));

                cancellationToken.Register(() => handler.TrySetException(new TaskCanceledException()));

                var keyBytes = keySerializer(topicPartition.Topic, message.Key);
                var valBytes = valueSerializer(topicPartition.Topic, message.Value);
                
                producer.ProduceImpl(
                    topicPartition.Topic, 
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, message.Headers, 
                    handler);

                return handler.Task;
            }
            else
            {
                var keyBytes = keySerializer(topicPartition.Topic, message.Key);
                var valBytes = valueSerializer(topicPartition.Topic, message.Value);
                
                producer.ProduceImpl(
                    topicPartition.Topic, 
                    valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                    keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                    message.Timestamp, topicPartition.Partition, message.Headers, 
                    null);

                var result = new DeliveryReport<TKey, TValue>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Invalid),
                    Message = message
                };

                return Task.FromResult(result);
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
        /// <param name="deliveryHandler">
        ///     A delegate that will be called with a delivery report corresponding
        ///     to the produce request (if enabled).
        /// </param>
        public void BeginProduce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null)
        {
            var keyBytes = keySerializer(topicPartition.Topic, message.Key);
            var valBytes = valueSerializer(topicPartition.Topic, message.Value);

            producer.ProduceImpl(
                topicPartition.Topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length, 
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length, 
                message.Timestamp, topicPartition.Partition, 
                message.Headers, 
                new TypedDeliveryHandlerShim_Action(
                    topicPartition.Topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey),
                    producer.enableDeliveryReportValue ? message.Value : default(TValue),
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
        public void BeginProduce(string topic, Message<TKey, TValue> message, Action<DeliveryReportResult<TKey, TValue>> deliveryHandler = null)
        {
            var keyBytes = keySerializer(topic, message.Key);
            var valBytes = valueSerializer(topic, message.Value);

            producer.ProduceImpl(
                topic,
                valBytes, 0, valBytes == null ? 0 : valBytes.Length,
                keyBytes, 0, keyBytes == null ? 0 : keyBytes.Length,
                message.Timestamp, Partition.Any,
                message.Headers,
                new TypedDeliveryHandlerShim_Action(
                    topic,
                    producer.enableDeliveryReportKey ? message.Key : default(TKey), 
                    producer.enableDeliveryReportValue ? message.Value : default(TValue), 
                    deliveryHandler)
            );
        }
    }



    /// <summary>
    ///     A high-level Apache Kafka producer (without serialization).
    /// </summary>
    internal class Producer : IClient
    {
        internal class UntypedDeliveryReport
        {
            /// <summary>
            ///     The topic associated with the message.
            /// </summary>
            public string Topic { get; set; }

            /// <summary>
            ///     The partition associated with the message.
            /// </summary>
            public Partition Partition { get; set; } = Partition.Any;

            /// <summary>
            ///     The partition offset associated with the message.
            /// </summary>
            public Offset Offset { get; set; } = Offset.Invalid;

            /// <summary>
            ///     An error (or NoError) associated with the message.
            /// </summary>
            public Error Error { get; set; }

            /// <summary>
            ///     The TopicPartition associated with the message.
            /// </summary>
            public TopicPartition TopicPartition
                => new TopicPartition(Topic, Partition);

            /// <summary>
            ///     The TopicPartitionOffset associated with the message.
            /// </summary>
            public TopicPartitionOffset TopicPartitionOffset
                => new TopicPartitionOffset(Topic, Partition, Offset);

            /// <summary>
            ///     The TopicPartitionOffsetError assoicated with the message.
            /// </summary>
            public TopicPartitionOffsetError TopicPartitionOffsetError
            {
                get
                {
                    return new TopicPartitionOffsetError(Topic, Partition, Offset, Error);
                }
                set
                {
                    Topic = value.Topic;
                    Partition = value.Partition;
                    Offset = value.Offset;
                    Error = value.Error;
                }
            }

            /// <summary>
            ///     The message that was produced.
            /// </summary>
            public Message Message { get; set; }
        }
        
        private bool disposeHasBeenCalled = false;
        private object disposeHasBeenCalledLockObj = new object();

        private readonly bool manualPoll = false;
        internal readonly bool enableDeliveryReports = true;
        internal readonly bool enableDeliveryReportHeaders = true;
        internal readonly bool enableDeliveryReportKey = true;
        internal readonly bool enableDeliveryReportValue = true;
        internal readonly bool enableDeliveryReportTimestamp = true;

        private readonly SafeKafkaHandle kafkaHandle;

        private readonly Task callbackTask;
        private readonly CancellationTokenSource callbackCts;

        private const int POLL_TIMEOUT_MS = 100;
        private Task StartPollTask(CancellationToken ct)
            => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (true)
                        {
                            ct.ThrowIfCancellationRequested();
                            kafkaHandle.Poll((IntPtr)POLL_TIMEOUT_MS);
                        }
                    }
                    catch (OperationCanceledException) {}
                }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);


        private readonly Librdkafka.ErrorDelegate errorCallbackDelegate;
        private void ErrorCallback(IntPtr rk, ErrorCode err, string reason, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }
            OnError?.Invoke(this, new ErrorEvent(new Error(err, reason), false));
        }


        private readonly Librdkafka.StatsDelegate statsCallbackDelegate;
        private int StatsCallback(IntPtr rk, IntPtr json, UIntPtr json_len, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return 0; }
            OnStatistics?.Invoke(this, Util.Marshal.PtrToStringUTF8(json));
            return 0; // instruct librdkafka to immediately free the json ptr.
        }


        private object loggerLockObj = new object();
        private readonly Librdkafka.LogDelegate logCallbackDelegate;
        private void LogCallback(IntPtr rk, SyslogLevel level, string fac, string buf)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            // Note: kafkaHandle can be null if the callback is during construction (in that case, we want the delegate to run).
            if (kafkaHandle != null && kafkaHandle.IsClosed) { return; }
            OnLog?.Invoke(this, new LogMessage(Util.Marshal.PtrToStringUTF8(Librdkafka.name(rk)), level, fac, buf));
        }

        private Librdkafka.DeliveryReportDelegate DeliveryReportCallback;

        /// <remarks>
        ///     note: this property is set to that defined in rd_kafka_conf
        ///     (which is never used by confluent-kafka-dotnet).
        /// </remarks>
        private void DeliveryReportCallbackImpl(IntPtr rk, IntPtr rkmessage, IntPtr opaque)
        {
            // Ensure registered handlers are never called as a side-effect of Dispose/Finalize (prevents deadlocks in common scenarios).
            if (kafkaHandle.IsClosed) { return; }

            var msg = Util.Marshal.PtrToStructureUnsafe<rd_kafka_message>(rkmessage);

            // the msg._private property has dual purpose. Here, it is an opaque pointer set
            // by Topic.Produce to be an IDeliveryHandler. When Consuming, it's for internal
            // use (hence the name).
            if (msg._private == IntPtr.Zero)
            {
                // Note: this can occur if the ProduceAsync overload that accepts a DeliveryHandler
                // was used and the delivery handler was set to null.
                return;
            }

            var gch = GCHandle.FromIntPtr(msg._private);
            var deliveryHandler = (IDeliveryHandler) gch.Target;
            gch.Free();

            Headers headers = null;
            if (this.enableDeliveryReportHeaders) 
            {
                headers = new Headers();
                Librdkafka.message_headers(rkmessage, out IntPtr hdrsPtr);
                if (hdrsPtr != IntPtr.Zero)
                {
                    for (var i=0; ; ++i)
                    {
                        var err = Librdkafka.header_get_all(hdrsPtr, (IntPtr)i, out IntPtr namep, out IntPtr valuep, out IntPtr sizep);
                        if (err != ErrorCode.NoError)
                        {
                            break;
                        }
                        var headerName = Util.Marshal.PtrToStringUTF8(namep);
                        byte[] headerValue = null;
                        if (valuep != IntPtr.Zero)
                        {
                            headerValue = new byte[(int)sizep];
                            Marshal.Copy(valuep, headerValue, 0, (int)sizep);
                        }
                        headers.Add(headerName, headerValue);
                    }
                }
            }

            IntPtr timestampType = (IntPtr)TimestampType.NotAvailable;
            long timestamp = 0;
            if (enableDeliveryReportTimestamp)
            {
                timestamp = Librdkafka.message_timestamp(rkmessage, out timestampType);
            }

            deliveryHandler.HandleDeliveryReport(
                new UntypedDeliveryReport 
                {
                    // Topic is not set here in order to avoid the marshalling cost.
                    // Instead, the delivery handler is expected to cache the topic string.
                    Partition = msg.partition, 
                    Offset = msg.offset, 
                    Error = msg.err,
                    Message = new Message { Timestamp = new Timestamp(timestamp, (TimestampType)timestampType), Headers = headers }
                }
            );
        }

        internal void ProduceImpl(
            string topic,
            byte[] val, int valOffset, int valLength,
            byte[] key, int keyOffset, int keyLength,
            Timestamp timestamp,
            Partition partition, 
            IEnumerable<Header> headers,
            IDeliveryHandler deliveryHandler)
        {
            if (timestamp.Type != TimestampType.CreateTime)
            {
                if (timestamp != Timestamp.Default)
                {
                    throw new ArgumentException("Timestamp must be either Timestamp.Default, or timestamp type must equal CreateTime.");
                }
            }

            if (this.enableDeliveryReports && deliveryHandler != null)
            {
                // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

                // Note: There is a level of indirection between the GCHandle and
                // physical memory address. GCHandle.ToIntPtr doesn't get the
                // physical address, it gets an id that refers to the object via
                // a handle-table.
                var gch = GCHandle.Alloc(deliveryHandler);
                var ptr = GCHandle.ToIntPtr(gch);

                var err = kafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    ptr);

                if (err != ErrorCode.NoError)
                {
                    gch.Free();
                    throw new KafkaException(err);
                }
            }
            else
            {
                var err = kafkaHandle.Produce(
                    topic,
                    val, valOffset, valLength,
                    key, keyOffset, keyLength,
                    partition.Value,
                    timestamp.UnixTimestampMs,
                    headers,
                    IntPtr.Zero);

                if (err != ErrorCode.NoError)
                {
                    throw new KafkaException(err);
                }
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnLog" />.
        /// </summary>
        public event EventHandler<LogMessage> OnLog;

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnError" />.
        /// </summary>
        public event EventHandler<ErrorEvent> OnError;

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.IClient.OnStatistics" />.
        /// </summary>
        public event EventHandler<string> OnStatistics;


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public Producer(IEnumerable<KeyValuePair<string, string>> config)
        {
            // TODO: Make Tasks auto complete when EnableDeliveryReportsPropertyName is set to false.
            // TODO: Hijack the "delivery.report.only.error" configuration parameter and add functionality to enforce that Tasks 
            //       that never complete are never created when this is set to true.

            this.DeliveryReportCallback = DeliveryReportCallbackImpl;

            Librdkafka.Initialize(null);

            var modifiedConfig = config
                .Where(prop => 
                    prop.Key != ConfigPropertyNames.ProducerEnableBackgroundPoll &&
                    prop.Key != ConfigPropertyNames.ProducerEnableDeliveryReports &&
                    prop.Key != ConfigPropertyNames.ProducerDeliveryReportFields);

            if (modifiedConfig.Where(obj => obj.Key == "delivery.report.only.error").Count() > 0)
            {
                // A managed object is kept alive over the duration of the produce request. If there is no
                // delivery report generated, there will be a memory leak. We could possibly support this 
                // property by keeping track of delivery reports in managed code, but this seems like 
                // more trouble than it's worth.
                throw new ArgumentException("The 'delivery.report.only.error' property is not supported by this client");
            }

            var enableBackgroundPollObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.ProducerEnableBackgroundPoll).Value;
            if (enableBackgroundPollObj != null)
            {
                this.manualPoll = !bool.Parse(enableBackgroundPollObj.ToString());
            }

            var enableDeliveryReportsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.ProducerEnableDeliveryReports).Value;
            if (enableDeliveryReportsObj != null)
            {
                this.enableDeliveryReports = bool.Parse(enableDeliveryReportsObj.ToString());
            }

            var deliveryReportEnabledFieldsObj = config.FirstOrDefault(prop => prop.Key == ConfigPropertyNames.ProducerDeliveryReportFields).Value;
            if (deliveryReportEnabledFieldsObj != null)
            {
                var fields = deliveryReportEnabledFieldsObj.ToString().Replace(" ", "");
                if (fields != "all")
                {
                    this.enableDeliveryReportKey = false;
                    this.enableDeliveryReportValue = false;
                    this.enableDeliveryReportHeaders = false;
                    this.enableDeliveryReportTimestamp = false;
                    if (fields != "none")
                    {
                        var parts = fields.Split(',');
                        foreach (var part in parts)
                        {
                            switch (part)
                            {
                                case "key": this.enableDeliveryReportKey = true; break;
                                case "value": this.enableDeliveryReportValue = true; break;
                                case "timestamp": this.enableDeliveryReportTimestamp = true; break;
                                case "headers": this.enableDeliveryReportHeaders = true; break;
                                default: throw new ArgumentException(
                                    $"Unexpected delivery report field name '{part}' in config value '{ConfigPropertyNames.ProducerDeliveryReportFields}'.");
                            }
                        }
                    }
                }
            }

            // Note: changing the default value of produce.offset.report at the binding level is less than
            // ideal since it means the librdkafka configuration docs will no longer completely match the 
            // .NET client. The default should probably be changed in librdkafka as well.
            if (modifiedConfig.FirstOrDefault(prop => prop.Key == "produce.offset.report").Value == null)
            {
                modifiedConfig = modifiedConfig.Concat(new KeyValuePair<string, string>[] { new KeyValuePair<string, string>("produce.offset.report", "true") });
            }

            var configHandle = SafeConfigHandle.Create();

            modifiedConfig.ToList().ForEach((kvp) => {
                if (kvp.Value == null) throw new ArgumentException($"'{kvp.Key}' configuration parameter must not be null.");
                configHandle.Set(kvp.Key, kvp.Value.ToString());
            });


            IntPtr configPtr = configHandle.DangerousGetHandle();

            if (enableDeliveryReports)
            {
                Librdkafka.conf_set_dr_msg_cb(configPtr, DeliveryReportCallback);
            }

            // Explicitly keep references to delegates so they are not reclaimed by the GC.
            errorCallbackDelegate = ErrorCallback;
            logCallbackDelegate = LogCallback;
            statsCallbackDelegate = StatsCallback;

            // TODO: provide some mechanism whereby calls to the error and log callbacks are cached until
            //       such time as event handlers have had a chance to be registered.
            Librdkafka.conf_set_error_cb(configPtr, errorCallbackDelegate);
            Librdkafka.conf_set_log_cb(configPtr, logCallbackDelegate);
            Librdkafka.conf_set_stats_cb(configPtr, statsCallbackDelegate);

            this.kafkaHandle = SafeKafkaHandle.Create(RdKafkaType.Producer, configPtr, this);
            configHandle.SetHandleAsInvalid(); // config object is no longer useable.

            if (!manualPoll)
            {
                callbackCts = new CancellationTokenSource();
                callbackTask = StartPollTask(callbackCts.Token);
            }
        }


        internal int Poll(int millisecondsTimeout)
        {
            if (!manualPoll)
            {
                throw new InvalidOperationException("Poll method called, but manual polling is not enabled.");
            }

            return this.kafkaHandle.Poll((IntPtr)millisecondsTimeout);
        }


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public int Poll(TimeSpan timeout)
            => Poll(timeout.TotalMillisecondsAsInt());


        internal int Flush(int millisecondsTimeout)
            => kafkaHandle.Flush(millisecondsTimeout);


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        internal int Flush(TimeSpan timeout)
            => kafkaHandle.Flush(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            // Calling Dispose a second or subsequent time should be a no-op.
            lock (disposeHasBeenCalledLockObj)
            { 
                if (disposeHasBeenCalled) { return; }
                disposeHasBeenCalled = true;
            }

            if (disposing)
            {
                if (!this.manualPoll)
                {
                    callbackCts.Cancel();
                    try
                    {
                        // Note: It's necessary to wait on callbackTask before disposing kafkaHandle
                        // since the poll loop makes use of this.
                        callbackTask.Wait();
                    }
                    catch (AggregateException e)
                    {
                        if (e.InnerException.GetType() != typeof(TaskCanceledException))
                        {
                            throw e.InnerException;
                        }
                    }
                    finally
                    {
                        callbackCts.Dispose();
                    }
                }

                // calls to rd_kafka_destroy may result in callbacks
                // as a side-effect. however the callbacks this class
                // registers with librdkafka ensure that any registered
                // events are not called if kafkaHandle has been closed.
                // this avoids deadlocks in common scenarios.
                kafkaHandle.Dispose();
            }
        }


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public string Name
            => kafkaHandle.Name;


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public int AddBrokers(string brokers)
            => kafkaHandle.AddBrokers(brokers);


        /// <summary>
        ///     <see cref="Confluent.Kafka.Producer{TKey, TValue}" />
        /// </summary>
        public Handle Handle 
            => new Handle { Owner = this, LibrdkafkaHandle = kafkaHandle };
    }
}
