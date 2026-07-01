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
        /// <param name="cancellationToken">
        ///     A cancellation token to observe whilst waiting
        ///     the returned task to complete.
        /// </param>
        /// <returns>
        ///     A Task which will complete with a delivery
        ///     report corresponding to the produce request,
        ///     or an exception if an error occured.
        /// </returns>
        /// <exception cref="Confluent.Kafka.ProduceException{TKey,TValue}">
        ///     Thrown in response to any produce request
        ///     that was unsuccessful for any reason
        ///     (excluding user application logic errors).
        ///     The Error property of the exception provides
        ///     more detailed information.
        /// </exception>
        /// <exception cref="System.ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));


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
        /// <param name="cancellationToken">
        ///     A cancellation token to observe whilst waiting
        ///     the returned task to complete.
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
        /// <exception cref="System.ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));


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
        /// <exception cref="Confluent.Kafka.ProduceException{TKey,TValue}">
        ///     Thrown in response to any error that is known
        ///     immediately (excluding user application logic
        ///     errors), for example ErrorCode.Local_QueueFull.
        ///     Asynchronous notification of unsuccessful produce
        ///     requests is made available via the <paramref name="deliveryHandler" />
        ///     parameter (if specified). The Error property of
        ///     the exception / delivery report provides more
        ///     detailed information.
        /// </exception>
        /// <exception cref="System.ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
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
        /// <exception cref="System.ArgumentException">
        ///     Thrown in response to invalid argument values.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
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
        /// <param name="cancellationToken">
        ///     A cancellation token to observe whilst waiting
        ///     the returned task to complete.
        /// </param>
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


        /// <summary>
        ///     Initialize transactions for the producer instance. 
        ///     This function ensures any transactions initiated by previous instances
        ///     of the producer with the same TransactionalId are completed.
        ///     If the previous instance failed with a transaction in progress the
        ///     previous transaction will be aborted.
        ///     This function needs to be called before any other transactional or
        ///     produce functions are called when the TransactionalId is configured.
        ///
        ///     If the last transaction had begun completion (following transaction commit)
        ///     but not yet finished, this function will await the previous transaction's
        ///     completion.
        ///
        ///     When any previous transactions have been fenced this function
        ///     will acquire the internal producer id and epoch, used in all future
        ///     transactional messages issued by this producer instance.
        ///
        ///     Upon successful return from this function the application has to perform at
        ///     least one of the following operations within TransactionalTimeoutMs to
        ///     avoid timing out the transaction on the broker:
        ///         * ProduceAsync (et.al)
        ///         * SendOffsetsToTransaction
        ///         * CommitTransaction
        ///         * AbortTransaction
        /// </summary>
        /// <param name="timeout">
        ///     The maximum length of time this method may block.
        /// </param>
        /// <exception cref="KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void InitTransactions(TimeSpan timeout);


        /// <summary>
        ///     Begin a new transaction.
        ///
        ///     InitTransactions must have been called successfully (once)
        ///     before this function is called.
        ///
        ///     Any messages produced, offsets sent (SendOffsetsToTransaction),
        ///     etc, after the successful return of this function will be part of
        ///     the transaction and committed or aborted atomatically.
        ///
        ///     Finish the transaction by calling CommitTransaction or
        ///     abort the transaction by calling AbortTransaction.
        /// </summary>
        /// <remark>
        ///     With the transactional producer, ProduceAsync and
        ///     Prodce calls are only allowed during an on-going
        ///     transaction, as started with this function.
        ///     Any produce call outside an on-going transaction,
        ///     or for a failed transaction, will fail.
        /// </remark>
        /// <exception cref="KafkaException">
        ///     Thrown on all errors.
        /// </exception>
        void BeginTransaction();


        /// <summary>
        ///     Commit the current transaction (as started with
        ///     BeginTransaction).
        ///
        ///     Any outstanding messages will be flushed (delivered) before actually
        ///     committing the transaction.
        ///
        ///     If any of the outstanding messages fail permanently the current
        ///     transaction will enter the abortable error state, in this case
        ///     the application must call AbortTransaction before attempting a new
        ///     transaction with BeginTransaction.
        ///
        ///     IMPORTANT NOTE: It is currently strongly recommended that the application
        ///     call CommitTransaction without specifying a timeout (which will block up
        ///     to the remaining transaction timeout - ProducerConfig.TransactionTimeoutMs)
        ///     because the Transactional Producer's API timeout handling is inconsistent with
        ///     the underlying protocol requests (known issue).
        /// </summary>
        /// <remark>
        ///      This function will block until all outstanding messages are
        ///      delivered and the transaction commit request has been successfully
        ///      handled by the transaction coordinator, or until <paramref name="timeout" />
        ///      expires, which ever comes first. On timeout the application may
        ///      call the function again.
        /// </remark>
        /// <remark>
        ///     Will automatically call Flush to ensure all queued
        ///     messages are delivered before attempting to commit the
        ///     transaction.
        /// </remark>
        /// <param name="timeout">
        ///     The maximum length of time this method may block.
        /// </param>
        /// <exception cref="KafkaTxnRequiresAbortException">
        ///     Thrown if the application must call AbortTransaction and
        ///     start a new transaction with BeginTransaction if it
        ///     wishes to proceed with transactions.
        /// </exception>
        /// <exception cref="KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void CommitTransaction(TimeSpan timeout);


        /// <summary>
        ///     Commit the current transaction (as started with
        ///     BeginTransaction).
        ///
        ///     Any outstanding messages will be flushed (delivered) before actually
        ///     committing the transaction.
        ///
        ///     If any of the outstanding messages fail permanently the current
        ///     transaction will enter the abortable error state, in this case
        ///     the application must call AbortTransaction before attempting a new
        ///     transaction with BeginTransaction.
        /// </summary>
        /// <remark>
        ///     This function will block until all outstanding messages are
        ///     delivered and the transaction commit request has been successfully
        ///     handled by the transaction coordinator, or until the transaction
        ///     times out (ProducerConfig.TransactionTimeoutMs) which ever comes
        ///     first. On timeout the application may call the function again.
        /// </remark>
        /// <remark>
        ///     Will automatically call Flush to ensure all queued
        ///     messages are delivered before attempting to commit the
        ///     transaction.
        /// </remark>
        /// <exception cref="KafkaTxnRequiresAbortException">
        ///     Thrown if the application must call AbortTransaction and
        ///     start a new transaction with BeginTransaction if it
        ///     wishes to proceed with transactions.
        /// </exception>
        /// <exception cref="KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void CommitTransaction();


        /// <summary>
        ///     Aborts the ongoing transaction.
        ///
        ///     This function should also be used to recover from non-fatal abortable
        ///     transaction errors.
        ///
        ///     Any outstanding messages will be purged and fail.
        ///
        ///     IMPORTANT NOTE: It is currently strongly recommended that the application
        ///     call AbortTransaction without specifying a timeout (which will block up
        ///     to the remaining transaction timeout - ProducerConfig.TransactionTimeoutMs)
        ///     because the Transactional Producer's API timeout handling is inconsistent with
        ///     the underlying protocol requests (known issue).
        /// </summary>
        /// <remark>
        ///     This function will block until all outstanding messages are purged
        ///     and the transaction abort request has been successfully
        ///     handled by the transaction coordinator, or until <paramref name="timeout" />
        ///     expires, which ever comes first. On timeout the application may
        ///     call the function again.
        /// </remark>
        /// <param name="timeout">
        ///     The maximum length of time this method may block.
        /// </param>
        /// <exception cref="KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void AbortTransaction(TimeSpan timeout);


        /// <summary>
        ///     Aborts the ongoing transaction.
        ///
        ///     This function should also be used to recover from non-fatal abortable
        ///     transaction errors.
        ///
        ///     Any outstanding messages will be purged and fail.
        /// </summary>
        /// <remark>
        ///     This function will block until all outstanding messages are purged
        ///     and the transaction abort request has been successfully
        ///     handled by the transaction coordinator, or until the transaction
        ///     times out (ProducerConfig.TransactionTimeoutMs), which ever comes
        ///     first. On timeout the application may call the function again.
        /// </remark>
        /// <exception cref="KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void AbortTransaction();


        /// <summary>
        ///     Sends a list of topic partition offsets to the consumer group
        ///     coordinator for <paramref name="groupMetadata" />, and marks
        ///     the offsets as part part of the current transaction.
        ///     These offsets will be considered committed only if the transaction is
        ///     committed successfully.
        ///
        ///     The offsets should be the next message your application will consume,
        ///     i.e., the last processed message's offset + 1 for each partition.
        ///     Either track the offsets manually during processing or use
        ///     Position property (on the consumer) to get the current offsets for
        ///     the partitions assigned to the consumer.
        ///
        ///     Use this method at the end of a consume-transform-produce loop prior
        ///     to committing the transaction with CommitTransaction.
        /// </summary>
        /// <remark>
        ///     The consumer must disable auto commits
        ///     (set EnableAutoCommit to false on the consumer).
        /// </remark>
        /// <remark>
        ///     Logical and invalid offsets (such as Offset.Unset) in
        ///     <paramref name="offsets" /> will be ignored, if there are no valid offsets in
        ///     <paramref name="offsets" /> the function will not throw
        ///     and no action will be taken.
        /// </remark>
        /// <param name="offsets">
        ///     List of offsets to commit to the consumer group upon
        ///     successful commit of the transaction. Offsets should be
        ///     the next message to consume, e.g., last processed message + 1.
        /// </param>
        /// <param name="groupMetadata">
        ///     The consumer group metadata acquired via
        ///     <see cref="IConsumer{K,V}.ConsumerGroupMetadata" />
        /// </param>
        /// <param name="timeout">
        ///     The maximum length of time this method may block.
        /// </param>
        /// <exception cref="System.ArgumentException">
        ///     Thrown if group metadata is invalid.
        /// </exception>
        /// <exception cref="Confluent.Kafka.KafkaTxnRequiresAbortException">
        ///     Thrown if the application must call AbortTransaction and
        ///     start a new transaction with BeginTransaction if it
        ///     wishes to proceed with transactions.
        /// </exception>
        /// <exception cref="Confluent.Kafka.KafkaRetriableException">
        ///     Thrown if an error occured, and the operation may be retried.
        /// </exception>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown on all other errors.
        /// </exception>
        void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout);
    }
}
