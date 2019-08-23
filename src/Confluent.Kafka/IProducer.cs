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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using System.Collections.Concurrent;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka producer client
    ///     that provides key and value serialization.
    /// </summary>
    public interface IProducer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Asynchronously send a single message to a
        ///     Kafka topic. The partition the message is
        ///     sent to is determined by the partitioner
        ///     defined using the 'partitioner' configuration
        ///     property.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <returns>
        ///     A Task which will complete with a delivery
        ///     report corresponding to the produce request,
        ///     or an exception if an error occured.
        /// </returns>
        /// <exception cref="ProduceException{TKey,TValue}">
        ///     Thrown in response to any produce request
        ///     that was unsuccessful for any reason
        ///     (excluding user application logic errors).
        ///     The Error property of the exception provides
        ///     more detailed information.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message);


        /// <summary>
        ///     Asynchronously send a single message to a
        ///     Kafka topic/partition.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic partition to produce the
        ///     message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <returns>
        ///     A Task which will complete with a delivery
        ///     report corresponding to the produce request,
        ///     or an exception if an error occured.
        /// </returns>
        /// <exception cref="ProduceException{TKey,TValue}">
        ///     Thrown in response to any produce request
        ///     that was unsuccessful for any reason
        ///     (excluding user application logic errors).
        ///     The Error property of the exception provides
        ///     more detailed information.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message);


        /// <summary>
        ///     Asynchronously send a single message to a
        ///     Kafka topic. The partition the message is sent
        ///     to is determined by the partitioner defined
        ///     using the 'partitioner' configuration property.
        /// </summary>
        /// <param name="topic">
        ///     The topic to produce the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called
        ///     with a delivery report corresponding to the
        ///     produce request (if enabled).
        /// </param>
        /// <exception cref="ProduceException{TKey,TValue}">
        ///     Thrown in response to any error that is known
        ///     immediately (excluding user application logic
        ///     errors), for example ErrorCode.Local_QueueFull.
        ///     Asynchronous notification of unsuccessful produce
        ///     requests is made available via the <paramref name="deliveryHandler" />
        ///     parameter (if specified). The Error property of
        ///     the exception / delivery report provides more
        ///     detailed information.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        ///     Thrown in response to error conditions that
        ///     reflect an error in the application logic of
        ///     the calling application.
        /// </exception>
        void Produce(
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);


        /// <summary>
        ///     Asynchronously send a single message to a
        ///     Kafka topic partition.
        /// </summary>
        /// <param name="topicPartition">
        ///     The topic partition to produce
        ///     the message to.
        /// </param>
        /// <param name="message">
        ///     The message to produce.
        /// </param>
        /// <param name="deliveryHandler">
        ///     A delegate that will be called
        ///     with a delivery report corresponding to the
        ///     produce request (if enabled).
        /// </param>
        /// <exception cref="ProduceException{TKey,TValue}">
        ///     Thrown in response to any error that is known
        ///     immediately (excluding user application logic errors),
        ///     for example ErrorCode.Local_QueueFull. Asynchronous
        ///     notification of unsuccessful produce requests is made
        ///     available via the <paramref name="deliveryHandler" />
        ///     parameter (if specified). The Error property of the
        ///     exception / delivery report provides more detailed
        ///     information.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        ///     Thrown in response to error conditions that reflect
        ///     an error in the application logic of the calling
        ///     application.
        /// </exception>
        void Produce(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);

        
        /// <summary>
        ///     Poll for callback events.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time to block if
        ///     no callback events are waiting. You should
        ///     typically use a relatively short timeout period
        ///     because this operation cannot be cancelled.
        /// </param>
        /// <returns>
        ///     Returns the number of events served since
        ///     the last call to this method or if this 
        ///     method has not yet been called, over the
        ///     lifetime of the producer.
        /// </returns>
        int Poll(TimeSpan timeout);


        /// <summary>
        ///     Wait until all outstanding produce requests and
        ///     delivery report callbacks are completed.
        ///    
        ///     [API-SUBJECT-TO-CHANGE] - the semantics and/or
        ///     type of the return value is subject to change.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time to block.
        ///     You should typically use a relatively short
        ///     timeout period and loop until the return value
        ///     becomes zero because this operation cannot be
        ///     cancelled. 
        /// </param>
        /// <returns>
        ///     The current librdkafka out queue length. This
        ///     should be interpreted as a rough indication of
        ///     the number of messages waiting to be sent to or
        ///     acknowledged by the broker. If zero, there are
        ///     no outstanding messages or callbacks.
        ///     Specifically, the value is equal to the sum of
        ///     the number of produced messages for which a
        ///     delivery report has not yet been handled and a
        ///     number which is less than or equal to the
        ///     number of pending delivery report callback
        ///     events (as determined by the number of
        ///     outstanding protocol requests).
        /// </returns>
        /// <remarks>
        ///     This method should typically be called prior to
        ///     destroying a producer instance to make sure all
        ///     queued and in-flight produce requests are
        ///     completed before terminating. The wait time is
        ///     bounded by the timeout parameter.
        ///    
        ///     A related configuration parameter is
        ///     message.timeout.ms which determines the
        ///     maximum length of time librdkafka attempts
        ///     to deliver a message before giving up and
        ///     so also affects the maximum time a call to
        ///     Flush may block.
        /// 
        ///     Where this Producer instance shares a Handle
        ///     with one or more other producer instances, the
        ///     Flush method will wait on messages produced by
        ///     the other producer instances as well.
        /// </remarks>
        int Flush(TimeSpan timeout);

        
        /// <summary>
        ///     Wait until all outstanding produce requests and
        ///     delivery report callbacks are completed.
        /// </summary>
        /// <remarks>
        ///     This method should typically be called prior to
        ///     destroying a producer instance to make sure all
        ///     queued and in-flight produce requests are 
        ///     completed before terminating. 
        ///    
        ///     A related configuration parameter is
        ///     message.timeout.ms which determines the
        ///     maximum length of time librdkafka attempts
        ///     to deliver a message before giving up and
        ///     so also affects the maximum time a call to
        ///     Flush may block.
        /// 
        ///     Where this Producer instance shares a Handle
        ///     with one or more other producer instances, the
        ///     Flush method will wait on messages produced by
        ///     the other producer instances as well.
        /// </remarks>
        /// <exception cref="System.OperationCanceledException">
        ///     Thrown if the operation is cancelled.
        /// </exception>
        void Flush(CancellationToken cancellationToken = default(CancellationToken));
    }
}
