// Copyright 2022 Confluent Inc.
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

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;


namespace Confluent.Kafka.Examples.ExactlyOnce
{
    /// <summary>
    ///     Error that is thrown if the transactional producer receives a
    ///     fatal error.
    /// </summary>
    public class ProcessorFatalError : Exception
    {
    }

    /// <summary>
    ///     Represents a single message to be sent
    ///     to an output topic.
    /// </summary>
    public class ProduceMessage<K, V>
    {
        /// <summary>
        ///     Name of the topic to produce to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     Message to produce.
        /// </summary>
        public Message<K, V> Message { get; set; }

        /// <summary>
        ///     Delivery handler for this message.
        /// </summary>
        public Action<DeliveryReport<K, V>> DeliveryHandler { get; set; } = null;
    }

    /// <summary>
    ///     The transactional processor allows to use a producer an consumer for transactions,
    ///     by recreating the producer when a fatal error happens, sending offsets to transaction
    ///     and handling aborts and retries correctly.
    /// </summary>
    public sealed class Processor<KC, VC, KP, VP> : IDisposable
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger<Processor<KC, VC, KP, VP>>();

        private IProducer<KP, VP> producer { get; set; }

        private List<ConsumeResult<KC, VC>> inputBatch = new();

        private List<ProduceMessage<KP, VP>> outputBatch = new();

        private bool recreateProducer = true;

        private DateTime? lastEndTransactionTime = null;

        private readonly ICollection<ProduceMessage<KP, VP>> EmptyList =
            new List<ProduceMessage<KP, VP>>(0).AsReadOnly();

        /// <summary>
        ///     Collection of input topic.
        /// </summary>
        public ICollection<string> InputTopics { get; set; }

        /// <summary>
        ///     Interval between retries.
        /// </summary>
        public TimeSpan RetryInterval { get; set; }

        /// <summary>
        ///     Timeout used for transactional operations.
        /// </summary>
        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        /// <summary>
        ///     Producer config.
        /// </summary>
        public ProducerConfig ProducerConfig { get; set; }

        /// <summary>
        ///     Timeout used for consume calls.
        /// </summary>
        public int ConsumeTimeout { get; set; } = 100;

        /// <summary>
        ///     Max messages to be committed.
        /// </summary>
        public int CommitMaxMessages { get; set; } = 1;

        /// <summary>
        ///     Max period between commits.
        /// </summary>
        public TimeSpan CommitPeriod { get; set; } = TimeSpan.MaxValue;

        /// <summary>
        ///     Recreate the producer on fatal errors like 
        ///     producer fenced and continue processing.
        /// </summary>
        public bool RecreateProducerOnFatalErrors { get; set; } = true;

        /// <summary>
        ///     Consumer configuration, don't set for a producer only
        ///     processor.
        /// </summary>
        public ConsumerConfig ConsumerConfig { get; set; }

        /// <summary>
        ///     Cancellation token source to stop the processor.
        /// </summary>
        public CancellationTokenSource CancellationTokenSource { get; set; }

        /// <summary>
        ///     Delegate for Subscribe.
        /// </summary>
        public delegate void SubscribeDelegate(IConsumer<KC, VC> consumer);

        /// <summary>
        ///     Delegate for Process.
        /// </summary>
        public delegate ICollection<ProduceMessage<KP, VP>> ProcessDelegate(ConsumeResult<KC, VC> consumeResult);

        /// <summary>
        ///     Delegate for IsCommitNeeded.
        /// </summary>
        public delegate bool IsCommitNeededDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                                    ICollection<ProduceMessage<KP, VP>> outputBatch);

        /// <summary>
        ///     Delegate for EndTransaction.
        /// </summary>
        public delegate void EndTransactionDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                                    ICollection<ProduceMessage<KP, VP>> outputBatch,
                                                    bool committed);

        /// <summary>
        ///     Subscribes the consumer to topics.
        /// </summary>
        public SubscribeDelegate Subscribe { get; set; }

        /// <summary>
        ///     Process a new message, or null if no consumer was configured,
        ///     applies required computation on that data and returns the list
        ///     of messages to be sent to the output topic.
        ///     
        ///     Any exception thrown will cause a rewind of the consumer
        ///     and an abort of current transaction.
        /// </summary>
        public ProcessDelegate Process { get; set; }

        /// <summary>
        ///     Given current batch of messages received and to be sent to 
        ///     the output topic, returns true if it's the moment to commit the
        ///     transaction.
        /// </summary>
        public IsCommitNeededDelegate IsCommitNeeded { get; set; }

        /// <summary>
        ///     Called at the end of transaction with the Kafka batch 
        ///     and the <paramref name="committed" /> param that says if
        ///     the transaction completed successfully. Here the application
        ///     must commit or abort any other transactions it has
        ///     ongoing.
        /// </summary>
        public EndTransactionDelegate EndTransaction { get; set; }


        public Processor()
        {
            Subscribe = new SubscribeDelegate(DefaultSubscribe);
            Process = new ProcessDelegate(DefaultProcess);
            IsCommitNeeded = new IsCommitNeededDelegate(DefaultIsCommitNeeded);
            EndTransaction = new EndTransactionDelegate(DefaultEndTransaction);
        }

        private void DefaultSubscribe(IConsumer<KC, VC> consumer)
        {
            consumer.Subscribe(InputTopics);
        }

        private ICollection<ProduceMessage<KP, VP>> DefaultProcess(ConsumeResult<KC, VC> consumeResult)
        {
            return EmptyList;
        }

        private bool DefaultIsCommitNeeded(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch)
        {
            return (inputBatch.Any() || outputBatch.Any()) &&
                (
                    inputBatch.Count >= CommitMaxMessages ||
                    outputBatch.Count >= CommitMaxMessages ||
                    DateTime.UtcNow > lastEndTransactionTime + CommitPeriod
                );
        }

        private void DefaultEndTransaction(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch,
                                           bool committed)
        {
        }

        private static void ThrowIfNotRetriable(string operation, KafkaException e, TimeSpan retryInterval)
        {
            var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var isRetriable = e is KafkaRetriableException;
            var isFatal = e.Error.IsFatal;
            logger.LogError("{Operation} Kafka Exception caught: '{Message}', IsFatal: {isFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}",
                            operation, e.Message, isFatal, txnRequiresAbort, isRetriable);
            if (isFatal || txnRequiresAbort || !isRetriable)
            {
                throw e;
            }
            Thread.Sleep(retryInterval);
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds,
        ///     call <paramref name="onKafkaException"/> if a <see cref="Confluent.Kafka.KafkaException" /> occurs.
        /// </summary>
        private void Retry(string operation, Action action, Action<string, KafkaException, TimeSpan> onKafkaException = null)
        {
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    action();
                    break;
                }
                catch (KafkaException e)
                {
                    onKafkaException?.Invoke(operation, e, RetryInterval);
                }
            }
        }

        /// <summary>
        ///     Commits transaction's produced and consumed messages.
        ///     Aborts transaction if an abortable exception is thrown,
        ///     recreates the producer if a fatal exception is thrown or
        ///     retries each operation that is throwing a retriable exception.
        /// </summary>
        private void CommitKafkaTransaction(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                            ICollection<ProduceMessage<KP, VP>> outputBatch,
                                            IConsumer<KC, VC> consumer)
        {

            if (ProducerConfig == null)
            {
                if (consumer != null)
                {
                    try
                    {
                        foreach (var message in inputBatch)
                        {
                            Retry("ConsumerStoreOffset", () => consumer.StoreOffset(message),
                                ThrowIfNotRetriable);
                        }
                        Retry("ConsumerCommit", () => consumer.Commit(),
                                ThrowIfNotRetriable);
                    }
                    catch (KafkaException e)
                    {
                        throw new KafkaTxnRequiresAbortException(e.Error);
                    }
                }
                return;
            }

            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    if (recreateProducer)
                    {
                        logger.LogInformation("(Re)creating producer");
                        if (producer != null) producer.Dispose();
                        producer = new ProducerBuilder<KP, VP>(ProducerConfig).Build();
                        producer.InitTransactions(LocalTransactionOperationTimeout);
                        recreateProducer = false;
                    }

                    logger.LogInformation("Calling BeginTransaction.");
                    producer.BeginTransaction();

                    logger.LogInformation("Producing {Count} messages.", outputBatch.Count);
                    foreach (var message in outputBatch)
                    {
                        Retry("Produce", () => producer.Produce(message.Topic,
                                                                message.Message,
                                                                message.DeliveryHandler), ThrowIfNotRetriable);
                    }

                    logger.LogInformation("Producing message completed");

                    if (consumer != null)
                    {
                        Retry("SendOffsetsToTransaction", () =>
                        {
                            logger.LogInformation("Calling SendOffsetsToTransaction");
                            producer.SendOffsetsToTransaction(
                                consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                consumer.ConsumerGroupMetadata,
                                LocalTransactionOperationTimeout);
                        }, ThrowIfNotRetriable);
                        logger.LogInformation("SendOffsetsToTransaction completed");
                    }

                    Retry("CommitTransaction", () =>
                    {
                        logger.LogInformation("calling CommitTransaction");
                        producer.CommitTransaction();
                    }, ThrowIfNotRetriable);

                    logger.LogInformation("CommitTransaction completed");
                    break;
                }
                catch (KafkaException e)
                {
                    logger.LogError("Kafka Exception caught, aborting transaction, trying again in {TotalSeconds} seconds: '{Message}'",
                                    RetryInterval.TotalSeconds,
                                    e.Message);
                    var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                    if (e.Error.IsFatal)
                    {
                        if (!RecreateProducerOnFatalErrors)
                            throw new ProcessorFatalError();
                        recreateProducer = true;
                    }
                    else if (TxnRequiresAbort)
                    {
                        Retry("AbortTransaction", () =>
                        {
                            logger.LogInformation("calling AbortTransaction");
                            producer.AbortTransaction(LocalTransactionOperationTimeout);
                        }, (operation, eInner, retryInterval) =>
                        {
                            var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                            var IsRetriableInner = eInner is KafkaRetriableException;
                            var IsFatalInner = eInner.Error.IsFatal;
                            logger.LogError("AbortTransaction Kafka Exception caught, trying again in {TotalSeconds} seconds: '{Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbortError}, IsRetriable: {IsRetriable}",
                                            RetryInterval.TotalSeconds, eInner.Message,
                                            IsFatalInner, TxnRequiresAbortErrorInner,
                                            IsRetriableInner);
                            Thread.Sleep(RetryInterval);
                            if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                            {
                                if (IsFatalInner)
                                {
                                    if (!RecreateProducerOnFatalErrors)
                                        throw new ProcessorFatalError();
                                    recreateProducer = true;
                                }
                                // Propagate abort to consumer and application
                                throw e;
                            }
                        });
                        // Propagate abort to consumer and application
                        throw;
                    }
                    Thread.Sleep(RetryInterval);
                }
                catch
                {
                    logger.LogError("Caught a different type of exception, this shouldn't happen'");
                    throw;
                }
            }
        }

        /// <summary>
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private void RewindConsumer(IConsumer<KC, VC> consumer, TimeSpan timeout)
        {
            foreach (var committedOffset in consumer.Committed(consumer.Assignment, timeout))
            {
                var position = committedOffset.Offset < 0
                               ? Offset.Beginning : committedOffset.Offset;
                consumer.Seek(new TopicPartitionOffset(committedOffset.TopicPartition, position));
            }
        }

        private void ResetTransaction()
        {
            lastEndTransactionTime = DateTime.UtcNow;
            inputBatch.Clear();
            outputBatch.Clear();
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This method should commit any application transaction too.
        /// </summary>
        private void CommitMaybe(IConsumer<KC, VC> consumer)
        {
            if (inputBatch.Any() || outputBatch.Any())
            {
                CommitKafkaTransaction(inputBatch, outputBatch, consumer);
                EndTransaction(inputBatch, outputBatch, true);
                ResetTransaction();
            }
        }

        private IConsumer<KC, VC> CreateConsumerMaybe()
        {
            if (ConsumerConfig != null)
            {
                ConsumerConfig.EnableAutoCommit = false;
                ConsumerConfig.EnableAutoOffsetStore = false;
                ConsumerConfig.IsolationLevel = IsolationLevel.ReadCommitted;

                return new ConsumerBuilder<KC, VC>(ConsumerConfig)
                    .SetPartitionsAssignedHandler((consumer, partitions) =>
                    {
                        logger.LogDebug("Assigned {Partitions}", partitions);
                    })
                    .SetPartitionsLostHandler((consumer, partitions) =>
                    {
                        // When partitions are lost, don't act.
                        logger.LogWarning("Partitions lost.");
                    })
                    .SetPartitionsRevokedHandler((consumer, partitions) =>
                    {
                        // When partitions are being revoked, commit transaction's remaining partitions
                        // before revoke is completed.
                        logger.LogInformation("Partitions revoked.");
                        if (!CancellationTokenSource.IsCancellationRequested)
                        {
                            CommitMaybe(consumer);
                        }
                    })
                    .Build();
            }
            return null;
        }

        /// <summary>
        ///     Runs the processor loop, calling process methods.
        /// </summary>
        public void RunLoop()
        {
            using var consumer = CreateConsumerMaybe();
            try
            {
                ResetTransaction();
                if (consumer != null) Subscribe(consumer);
                while (!CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<KC, VC> consumeResult = null;
                        if (consumer != null)
                        {
                            consumeResult = consumer.Consume(ConsumeTimeout);
                            if (consumeResult != null)
                            {
                                inputBatch.Add(consumeResult);
                            }
                        }
                        var messages = Process(consumeResult);
                        outputBatch.AddRange(messages);
                        var commitNeeded = IsCommitNeeded(inputBatch, outputBatch);
                        if (commitNeeded)
                        {
                            CommitMaybe(consumer);
                        }
                    }
                    catch (KafkaException e)
                    {
                        var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
                        logger.LogError("Consumer KafkaException exception: {Message}, TxnRequiresAbort: {TxnRequiresAbort}",
                                        e.Message, txnRequiresAbort);
                        if (txnRequiresAbort)
                        {
                            if (consumer != null)
                            {
                                Retry("Rewind",
                                    () => RewindConsumer(consumer, LocalTransactionOperationTimeout),
                                    (operation, e, retryInterval) =>
                                    {
                                        Thread.Sleep(RetryInterval);
                                    });
                            }
                            EndTransaction(inputBatch, outputBatch, false);
                            ResetTransaction();
                            Thread.Sleep(RetryInterval);
                        }
                        else
                        {
                            // This shouldn't happen
                            throw;
                        }
                    }
                    catch (Exception e)
                    {
                        if (e is ProcessorFatalError) throw;

                        logger.LogError("Process exception: {Message}",
                                        e.Message);
                        // Customer exception requires rewind and abort.
                        if (consumer != null)
                        {
                            Retry("Rewind",
                                () => RewindConsumer(consumer, LocalTransactionOperationTimeout),
                                (operation, e, retryInterval) =>
                                {
                                    Thread.Sleep(RetryInterval);
                                });
                        }
                        EndTransaction(inputBatch, outputBatch, false);
                        ResetTransaction();
                        Thread.Sleep(RetryInterval);
                    }
                }
            }
            finally
            {
                if (consumer != null)
                {
                    consumer.Close();
                }
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            if (producer != null) producer.Dispose();
        }
    }
}
