using System;
using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines Apache Kafka transactional producer client
    /// </summary>
    public interface ITransactionalProducer
    {
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
        /// <exception cref="ArgumentException">
        ///     Thrown if group metadata is invalid.
        /// </exception>
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
        void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout);
    }
}