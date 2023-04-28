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
using System.Threading.Tasks;
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
    }

    /// <summary>
    ///     The transactional processor allows to use a producer an consumer for transactions,
    ///     by recreating the producer when a fatal error happens, sending offsets to transaction
    ///     and handling aborts and retries correctly.
    /// </summary>
    public sealed class Processor<KC, VC, KP, VP> : IDisposable
    {
        private static readonly ILogger Logger = Utils.LoggerFactory.CreateLogger<Processor<KC, VC, KP, VP>>();

        private IProducer<KP, VP> Producer { get; set; }
        
        private IConsumer<KC, VC> Consumer { get; set; }

        private readonly List<ConsumeResult<KC, VC>> InputBatch = new();

        private readonly List<ProduceMessage<KP, VP>> OutputBatch = new();
        
        private readonly List<DeliveryResult<KP, VP>> DeliveryReportsBatch = new();

        private bool RecreateProducer = true;
        
        private static readonly int PhaseNotStarted = 0;
        private static readonly int PhaseInitTransaction = 1;
        private static readonly int PhaseAccumulateBatch = 2;
        private static readonly int PhaseCommit = 3;
        private static readonly int PhaseFatalError = 4;
        
        private int CurrentPhase = PhaseNotStarted;

        private DateTime? LastEndTransactionTime = null;

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
        public delegate Task SubscribeDelegate(IConsumer<KC, VC> consumer);

        /// <summary>
        ///     Delegate for Process.
        /// </summary>
        public delegate Task<ICollection<ProduceMessage<KP, VP>>> ProcessDelegate(ConsumeResult<KC, VC> consumeResult);

        /// <summary>
        ///     Delegate for IsCommitNeeded.
        /// </summary>
        public delegate Task<bool> IsCommitNeededDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                                    ICollection<ProduceMessage<KP, VP>> outputBatch);

        /// <summary>
        ///     Delegate for EndTransaction.
        /// </summary>
        public delegate Task EndTransactionDelegate(ICollection<ConsumeResult<KC, VC>> inputBatch,
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

        private Task DefaultSubscribe(IConsumer<KC, VC> consumer)
        {
            consumer.Subscribe(InputTopics);
            return Task.CompletedTask;
        }

        private Task<ICollection<ProduceMessage<KP, VP>>> DefaultProcess(ConsumeResult<KC, VC> consumeResult)
        {
            return Task.FromResult(EmptyList);
        }

        private Task<bool> DefaultIsCommitNeeded(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch)
        {
            bool isCommitNeeded = (inputBatch.Any() || outputBatch.Any()) &&
                (
                    inputBatch.Count >= CommitMaxMessages ||
                    outputBatch.Count >= CommitMaxMessages ||
                    DateTime.UtcNow > LastEndTransactionTime + CommitPeriod
                );
            return Task.FromResult(isCommitNeeded);
        }

        private Task DefaultEndTransaction(ICollection<ConsumeResult<KC, VC>> inputBatch,
                                           ICollection<ProduceMessage<KP, VP>> outputBatch,
                                           bool committed)
        {
            return Task.CompletedTask;
        }

        private static async Task ThrowIfNotRetriable(string operation, KafkaException e, TimeSpan retryInterval)
        {
            var txnRequiresAbort = e is KafkaTxnRequiresAbortException;
            var isRetriable = e is KafkaRetriableException;
            var isFatal = e.Error.IsFatal;
            Logger.LogError("{Operation} Kafka Exception caught: '{Message}', IsFatal: {isFatal}, TxnRequiresAbort: {TxnRequiresAbort}, IsRetriable: {IsRetriable}",
                            operation, e.Message, isFatal, txnRequiresAbort, isRetriable);
            if (isFatal || txnRequiresAbort || !isRetriable)
            {
                throw e;
            }
            await Task.Delay(retryInterval);
        }

        /// <summary>
        ///     Retry executing <paramref name="action"/> until it succeeds,
        ///     call <paramref name="onKafkaException"/> if a <see cref="Confluent.Kafka.KafkaException" /> occurs.
        /// </summary>
        private async Task Retry(string operation, Func<Task> action, Func<string, KafkaException, TimeSpan, Task> onKafkaException = null)
        {
            while (!CancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    await action();
                    break;
                }
                catch (KafkaException e)
                {
                    await onKafkaException?.Invoke(operation, e, RetryInterval);
                }
            }
        }
        
        private async Task<DeliveryResult<KP, VP>> ProduceMessage(ProduceMessage<KP, VP> message)
        {
            DeliveryResult<KP, VP> dr = null;
            await Retry("Produce", async () => 
                {
                    dr = await Producer.ProduceAsync(message.Topic, message.Message);
                }, ThrowIfNotRetriable);
            return dr;
        }
        
        private async Task<ICollection<DeliveryResult<KP, VP>>> ProduceBatch(ICollection<ProduceMessage<KP, VP>> batch)
        {
            Logger.LogInformation("Producing {Count} messages.", batch.Count);
            ICollection<DeliveryResult<KP, VP>> drs = new List<DeliveryResult<KP, VP>>();
            
            foreach (var message in batch)
            {
                drs.Add(await ProduceMessage(message));
            }

            Logger.LogInformation("Producing messages completed");
            return drs;
        }

        /// <summary>
        ///     Commits transaction's produced and consumed messages.
        ///     Aborts transaction if an abortable exception is thrown,
        ///     recreates the producer if a fatal exception is thrown or
        ///     retries each operation that is throwing a retriable exception.
        /// </summary>
        private async Task CommitKafkaTransaction(ICollection<ConsumeResult<KC, VC>> inputBatch)
        {
            // Start commit phase
            CurrentPhase = PhaseCommit;
            
            if (ProducerConfig == null)
            {
                if (Consumer != null)
                {
                    try
                    {
                        foreach (var message in inputBatch)
                        {
                            await Retry("ConsumerStoreOffset",
                                () => 
                                {
                                    Consumer.StoreOffset(message);
                                    return Task.CompletedTask;
                                },
                                ThrowIfNotRetriable);
                        }
                        await Retry("ConsumerCommit",
                                () =>
                                {
                                    Consumer.Commit();
                                    return Task.CompletedTask;
                                },
                                ThrowIfNotRetriable);
                    }
                    catch (KafkaException e)
                    {
                        throw new KafkaTxnRequiresAbortException(e.Error);
                    }
                }
                return;
            }

            if (Consumer != null)
            {
                await Retry("SendOffsetsToTransaction", () =>
                {
                    Logger.LogInformation("Calling SendOffsetsToTransaction");
                    Producer.SendOffsetsToTransaction(
                        Consumer.Assignment.Select(a =>
                            new TopicPartitionOffset(a, Consumer.Position(a))),
                        Consumer.ConsumerGroupMetadata,
                        LocalTransactionOperationTimeout);
                    return Task.CompletedTask;
                }, ThrowIfNotRetriable);
                Logger.LogInformation("SendOffsetsToTransaction completed");
            }

            await Retry("CommitTransaction", () =>
            {
                Logger.LogInformation("calling CommitTransaction");
                Producer.CommitTransaction();
                return Task.CompletedTask;
            }, ThrowIfNotRetriable);

            Logger.LogInformation("CommitTransaction completed");
        }

        /// <summary>
        ///     Seeks assigned partitions to last committed offset,
        ///     or to the earliest offset if no offset was committed yet.
        /// </summary>
        private static void RewindConsumer(IConsumer<KC, VC> consumer, TimeSpan timeout)
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
            LastEndTransactionTime = DateTime.UtcNow;
            InputBatch.Clear();
            OutputBatch.Clear();
            DeliveryReportsBatch.Clear();
            CurrentPhase = PhaseInitTransaction;
        }

        /// <summary>
        ///     Sends and commits accumulated messages, if any.
        ///     This method should commit any application transaction too.
        /// </summary>
        private async Task CommitMaybe()
        {
            if (InputBatch.Any() || OutputBatch.Any())
            {
                await DoCommit();
            }
        }
        
        private async Task DoCommit()
        {
            await CommitKafkaTransaction(InputBatch);
            await CompleteTransaction(true);
        }
        
        private void OnPartitionsAssigned(IConsumer<KC, VC> consumer, List<TopicPartition> partitions)
        {
            Logger.LogDebug("Assigned {Partitions}", partitions);
        }

        private void OnPartitionsLost(IConsumer<KC, VC> _, List<TopicPartitionOffset> _2)
        {
            // When partitions are lost, don't act.
            Logger.LogWarning("Partitions lost.");
        }
        
        private void OnPartitionsRevoked(IConsumer<KC, VC> _, List<TopicPartitionOffset> _2)
        {
            // When partitions are being revoked, commit transaction's remaining partitions
            // before revoke is completed.
            Logger.LogInformation("Partitions revoked.");
            if (!CancellationTokenSource.IsCancellationRequested)
            {
                CommitMaybe().GetAwaiter().GetResult();
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
                    .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                    .SetPartitionsLostHandler(OnPartitionsLost)
                    .SetPartitionsRevokedHandler(OnPartitionsRevoked)
                    .Build();
            }
            return null;
        }
        
        private async Task ConsumerRewind()
        {
            if (Consumer != null)
            {
                await Retry("Rewind",
                    () =>
                    {
                        RewindConsumer(Consumer, LocalTransactionOperationTimeout);
                        return Task.CompletedTask;
                    },
                    (operation, e, retryInterval) => Task.Delay(RetryInterval));
            }
        }
        
        private async Task CompleteTransaction(bool commit)
        {
            if (!commit)
            {
                await ConsumerRewind();
            }
            try
            {
                await EndTransaction(InputBatch, OutputBatch, commit);
            }
            catch (Exception e)
            {
                Logger.LogError("EndTransaction exception: {Message}",
                    e.Message);
            }
            ResetTransaction();
        }
        
        private async Task RestartKafkaTransaction()
        {
            if (CurrentPhase == PhaseInitTransaction ||
                CurrentPhase == PhaseCommit)
            {
                if (RecreateProducer)
                {
                    Logger.LogInformation("(Re)creating producer");
                    Producer?.Dispose();
                    Producer = new ProducerBuilder<KP, VP>(ProducerConfig).Build();
                    Producer.InitTransactions(LocalTransactionOperationTimeout);
                    RecreateProducer = false;
                }
                
                Logger.LogInformation("Calling BeginTransaction.");
                await Retry("BeginTransaction",
                      () =>
                      {
                        Producer.BeginTransaction();
                        return Task.CompletedTask;
                      },
                      ThrowIfNotRetriable);
                    
                if (CurrentPhase == PhaseInitTransaction)
                {
                    CurrentPhase = PhaseAccumulateBatch;
                }
            }
        }
        
        private async Task AbortKafkaTransaction()
        {
            await Retry("AbortTransaction", () =>
                {
                    Logger.LogInformation("calling AbortTransaction");
                    Producer.AbortTransaction(LocalTransactionOperationTimeout);
                    return Task.CompletedTask;
                }, async (operation, eInner, retryInterval) =>
                {
                    var TxnRequiresAbortErrorInner = eInner is KafkaTxnRequiresAbortException;
                    var IsRetriableInner = eInner is KafkaRetriableException;
                    var IsFatalInner = eInner.Error.IsFatal;
                    Logger.LogError("AbortTransaction Kafka Exception caught, trying again in {TotalMilliseconds} seconds: '{Message}', IsFatal: {IsFatal}, TxnRequiresAbort: {TxnRequiresAbortError}, IsRetriable: {IsRetriable}",
                                    RetryInterval.TotalMilliseconds, eInner.Message,
                                    IsFatalInner, TxnRequiresAbortErrorInner,
                                    IsRetriableInner);
                    if (!TxnRequiresAbortErrorInner && !IsRetriableInner)
                    {
                        if (IsFatalInner)
                        {
                            await FatalError();
                        }
                        // Propagate abort to consumer and application
                        await CompleteTransaction(false);
                    }
                    await Task.Delay(RetryInterval);
                });
        }

        private async Task FatalError()
        {
            if (!RecreateProducerOnFatalErrors)
            {
                // Propagate abort to consumer and application
                await CompleteTransaction(false);
                CurrentPhase = PhaseFatalError;
                throw new ProcessorFatalError();
            }
            RecreateProducer = true;
        }
        
        private int CASCurrentPhase(int from, int to)
        {
            return Interlocked.CompareExchange(ref CurrentPhase, to, from);
        }

        /// <summary>
        ///     Runs the processor loop, calling process methods.
        /// </summary>
        public async Task Start()
        {
            if (CASCurrentPhase(PhaseNotStarted, PhaseInitTransaction) != PhaseNotStarted)
            {
                throw new InvalidOperationException("processor already started");
            }
            
            Consumer = CreateConsumerMaybe();
            try
            {
                ResetTransaction();
                if (Consumer != null) await Subscribe(Consumer);
                while (!CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        await RestartKafkaTransaction();
                        
                        if (CurrentPhase == PhaseCommit)
                        {
                            DeliveryReportsBatch.AddRange(
                                await ProduceBatch(OutputBatch));
                            await DoCommit();
                        }
                        else
                        {
                            ConsumeResult<KC, VC> consumeResult = null;
                            if (Consumer != null)
                            {
                                consumeResult = Consumer.Consume(ConsumeTimeout);
                                if (consumeResult != null)
                                {
                                    InputBatch.Add(consumeResult);
                                }
                            }
                            var messages = await Process(consumeResult);
                            OutputBatch.AddRange(messages);
                            DeliveryReportsBatch.AddRange(
                                await ProduceBatch(messages));

                            var commitNeeded = await IsCommitNeeded(InputBatch, OutputBatch);
                            if (commitNeeded)
                            {
                                await CommitMaybe();
                            }
                        }
                    }
                    catch (KafkaException e)
                    {
                        Logger.LogError("Kafka Exception caught, aborting transaction, trying again in {TotalMilliseconds} ms: '{Message}'",
                            RetryInterval.TotalMilliseconds,
                            e.Message);
                        var TxnRequiresAbort = e is KafkaTxnRequiresAbortException;
                        if (e.Error.IsFatal)
                        {
                            await FatalError();
                        }
                        else if (TxnRequiresAbort)
                        {
                            await AbortKafkaTransaction();
                        }
                        // Propagate abort to consumer and application
                        await CompleteTransaction(false);
                        await Task.Delay(RetryInterval);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError("Process exception: {Message}",
                                        e.Message);
                        // Propagate abort to consumer and application
                        await CompleteTransaction(false);
                        await Task.Delay(RetryInterval);
                    }
                }
            }
            finally
            {
                Consumer?.Close();
            }
        }

        /// <summary>
        ///     Implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            Producer?.Dispose();
        }
    }
}
