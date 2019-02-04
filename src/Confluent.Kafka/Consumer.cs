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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serdes;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer with
    ///     deserialization capability.
    /// </summary>
    public class Consumer<TKey, TValue> : ConsumerBase, IConsumer<TKey, TValue>
    {
        /// <summary>
        ///     Set the partitions assigned handler.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.ConsumerBase.Assign(IEnumerable{TopicPartition})" />
        ///     method (or another overload of this method) in this handler, or do not specify a partitions assigned handler,
        ///     the consumer will be automatically assigned to the partition assignment set provided by the consumer group and
        ///     consumption will resume from the last committed offset for each partition, or if there is no committed offset,
        ///     in accordance with the `auto.offset.reset` configuration property. This default behavior will not occur if
        ///     you call Assign yourself in the handler. The set of partitions you assign to is not required to match the
        ///     assignment provided by the consumer group, but typically will.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        /// </remarks>
        public void SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> value)
        {
            if (value == null)
            {
                base.partitionsAssignedHandler = null;
                return;
            }
            base.partitionsAssignedHandler = partitions => value(this, partitions);
        }


        /// <summary>
        ///     Set the partitions revoked handler.
        /// 
        ///     If you do not call the <see cref="Confluent.Kafka.ConsumerBase.Unassign" /> or 
        ///     <see cref="Confluent.Kafka.ConsumerBase.Assign(IEnumerable{TopicPartition})" />
        ///     (or other overload) method in your handler, all partitions will be  automatically
        ///     unassigned. This default behavior will not occur if you call Unassign (or Assign)
        ///     yourself.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        /// </remarks>
        public void SetPartitionsRevokedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> value)
        {
            if (value == null)
            {
                base.partitionsRevokedHandler = null;
                return;
            }
            base.partitionsRevokedHandler = partitions => value(this, partitions);
        }


        /// <summary>
        ///     A handler that is called to report the result of (automatic) offset 
        ///     commits. It is not called as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        /// </remarks>
        public void SetOffsetsCommittedHandler(Action<IConsumer<TKey, TValue>, CommittedOffsets> value)
        {
            if (value == null)
            {
                base.offsetsCommittedHandler = null;
                return;
            }
            base.offsetsCommittedHandler = offsets => value(this, offsets);
        }


        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;
        private IAsyncDeserializer<TKey> asyncKeyDeserializer;
        private IAsyncDeserializer<TValue> asyncValueDeserializer;

        private Dictionary<Type, object> defaultDeserializers = new Dictionary<Type, object>
        {
            { typeof(Null), Deserializers.Null },
            { typeof(Ignore), Deserializers.Ignore },
            { typeof(int), Deserializers.Int32 },
            { typeof(long), Deserializers.Int64 },
            { typeof(string), Deserializers.Utf8 },
            { typeof(float), Deserializers.Single },
            { typeof(double), Deserializers.Double },
            { typeof(byte[]), Deserializers.ByteArray }
        };

        internal Consumer(ConsumerBuilder<TKey, TValue> builder)
        {
            base.Initialize(builder.ConstructBaseConfig(this));

            // setup key deserializer.
            if (builder.KeyDeserializer == null && builder.AsyncKeyDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TKey), out object deserializer))
                {
                    throw new ArgumentNullException(
                        $"Key deserializer not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.keyDeserializer = (IDeserializer<TKey>)deserializer;
            }
            else if (builder.KeyDeserializer == null && builder.AsyncKeyDeserializer != null)
            {
                this.asyncKeyDeserializer = builder.AsyncKeyDeserializer;
            }
            else if (builder.KeyDeserializer != null && builder.AsyncKeyDeserializer == null)
            {
                this.keyDeserializer = builder.KeyDeserializer;
            }
            else
            {
                throw new ArgumentException("Invalid key deserializer configuration.");
            }

            // setup value deserializer.
            if (builder.ValueDeserializer == null && builder.AsyncValueDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TValue), out object deserializer))
                {
                    throw new ArgumentNullException(
                        $"Key deserializer not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.valueDeserializer = (IDeserializer<TValue>)deserializer;
            }
            else if (builder.ValueDeserializer == null && builder.AsyncValueDeserializer != null)
            {
                this.asyncValueDeserializer = builder.AsyncValueDeserializer;
            }
            else if (builder.ValueDeserializer != null && builder.AsyncValueDeserializer == null)
            {
                this.valueDeserializer = builder.ValueDeserializer;
            }
            else
            {
                throw new ArgumentException("Invalid value deserializer configuration.");
            }
        }

        private ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            // TODO: add method(s) to ConsumerBase to handle the async case more optimally.
            var rawResult = base.Consume(millisecondsTimeout, Deserializers.ByteArray, Deserializers.ByteArray);
            if (rawResult == null) { return null; }
            if (rawResult.Message == null)
            {
                return new ConsumeResult<TKey, TValue>
                {
                    TopicPartitionOffset = rawResult.TopicPartitionOffset,
                    Message = null,
                    IsPartitionEOF = rawResult.IsPartitionEOF // always true
                };
            }

            TKey key = keyDeserializer != null
                ? keyDeserializer.Deserialize(rawResult.Key, rawResult.Key == null, true, rawResult.Message, rawResult.TopicPartition)
                : asyncKeyDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Key), rawResult.Key == null, true, rawResult.Message, rawResult.TopicPartition)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            TValue val = valueDeserializer != null
                ? valueDeserializer.Deserialize(rawResult.Value, rawResult.Value == null, false, rawResult.Message, rawResult.TopicPartition)
                : asyncValueDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Value), rawResult == null, false, rawResult.Message, rawResult.TopicPartition)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            return new ConsumeResult<TKey, TValue>
            {
                TopicPartitionOffset = rawResult.TopicPartitionOffset,
                Message = rawResult.Message == null ? null : new Message<TKey, TValue>
                {
                    Key = key,
                    Value = val,
                    Headers = rawResult.Headers,
                    Timestamp = rawResult.Timestamp
                },
                IsPartitionEOF = rawResult.IsPartitionEOF
            };
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                // Note: An alternative to throwing on cancellation is to return null,
                // but that would be problematic downstream (require null checks).
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeResult<TKey, TValue> result = (keyDeserializer != null && valueDeserializer != null)
                    ? Consume<TKey, TValue>(cancellationDelayMaxMs, keyDeserializer, valueDeserializer) // fast path for simple case.
                    : Consume(cancellationDelayMaxMs);

                if (result == null) { continue; }
                return result;
            }
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
            => (keyDeserializer != null && valueDeserializer != null)
                ? Consume<TKey, TValue>(timeout.TotalMillisecondsAsInt(), keyDeserializer, valueDeserializer) // fast path for simple case
                : Consume(timeout.TotalMillisecondsAsInt());


        /// <summary>
        ///     Commits an offset based on the topic/partition/offset of a ConsumeResult.
        /// </summary>
        /// <param name="result">
        ///     The ConsumeResult instance used to determine the committed offset.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation
        ///     (currently ignored).
        /// </param>
        /// <exception cref="Confluent.Kafka.KafkaException">
        ///     Thrown if the request failed.
        /// </exception>
        /// <exception cref="Confluent.Kafka.TopicPartitionOffsetException">
        ///     Thrown if the result is in error.
        /// </exception>
        /// <remarks>
        ///     A consumer which has position N has consumed messages with offsets up to N-1 
        ///     and will next receive the message with offset N. Hence, this method commits an 
        ///     offset of <paramref name="result" />.Offset + 1.
        /// </remarks>
        public void Commit(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (result.Message == null)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an empty consume result");
            }

            Commit(new [] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });
        }
    }


    /// <summary>
    ///     Implements a high-level Apache Kafka consumer.
    /// </summary>
    public class Consumer : ConsumerBase, IConsumer
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsAssignedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        public void SetPartitionsAssignedHandler(Action<IConsumer, List<TopicPartition>> value)
        {
            if (value == null)
            {
                base.partitionsAssignedHandler = null;
                return;
            }
            base.partitionsAssignedHandler = partitions => value(this, partitions);
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsRevokedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        public void SetPartitionsRevokedHandler(Action<IConsumer, List<TopicPartition>> value)
        {
            if (value == null)
            {
                base.partitionsRevokedHandler = null;
                return;
            }
            base.partitionsRevokedHandler = partitions => value(this, partitions);
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetOffsetsCommittedHandler(Action{IConsumer{TKey,TValue},CommittedOffsets})" />.
        /// </summary>
        public void SetOffsetsCommittedHandler(Action<IConsumer, CommittedOffsets> value)
        {
            if (value == null)
            {
                base.offsetsCommittedHandler = null;
                return;
            }
            base.offsetsCommittedHandler = offsets => value(this, offsets);
        }


        internal Consumer(ConsumerBuilder builder)
        {
            base.Initialize(builder.ConstructBaseConfig(this));
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the operation has been cancelled.
        /// </summary>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used to cancel this operation.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult Consume(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                // Note: An alternative to throwing on cancellation is to return null,
                // but that would be problematic downstream (require null checks).
                cancellationToken.ThrowIfCancellationRequested();
                var result = Consume(cancellationDelayMaxMs, Deserializers.ByteArray, Deserializers.ByteArray);
                if (result == null) { continue; }
                return new ConsumeResult
                {
                    TopicPartitionOffset = result.TopicPartitionOffset,
                    Message = result.Message,
                    IsPartitionEOF = result.IsPartitionEOF
                };
            }
        }


        /// <summary>
        ///     Poll for new messages / events. Blocks until a consume result
        ///     is available or the timeout period has elapsed.
        /// </summary>
        /// <param name="timeout">
        ///     The maximum period of time the call may block.
        /// </param>
        /// <returns>
        ///     The consume result.
        /// </returns>
        /// <remarks>
        ///     OnPartitionsAssigned/Revoked and OnOffsetsCommitted events may
        ///     be invoked as a side-effect of calling this method (on the same
        ///     thread).
        /// </remarks>
        public ConsumeResult Consume(TimeSpan timeout)
        {
            var result = Consume(timeout.TotalMillisecondsAsInt(), Deserializers.ByteArray, Deserializers.ByteArray);
            if (result == null) { return null; }
            return new ConsumeResult
            {
                TopicPartitionOffset = result.TopicPartitionOffset,
                Message = result.Message,
                IsPartitionEOF = result.IsPartitionEOF
            };
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Commit(ConsumeResult{TKey, TValue}, CancellationToken)" />
        /// </summary>
        public void Commit(ConsumeResult result, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (result.Message == null)
            {
                throw new InvalidOperationException("Attempt was made to commit offset corresponding to an empty consume result");
            }

            Commit(new [] { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });
        }

    }
}
