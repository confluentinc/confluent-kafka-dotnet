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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implements a high-level Apache Kafka consumer with
    ///     deserialization capability.
    /// </summary>
    public class Consumer<TKey, TValue> : ConsumerBase, IConsumer<TKey, TValue>
    {
        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;
        private IAsyncDeserializer<TKey> taskKeyDeserializer;
        private IAsyncDeserializer<TValue> taskValueDeserializer;

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

        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Consumer{TKey,TValue}" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        /// <param name="keyDeserializer">
        ///     The deserializer to use to deserialize keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     The deserializer to use to deserialize values.
        /// </param>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            IDeserializer<TKey> keyDeserializer = null,
            IDeserializer<TValue> valueDeserializer = null
        ) : base(config)
        {
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;

            if (keyDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TKey), out object deserializer))
                {
                    throw new ArgumentNullException(
                        $"Key deserializer not specified and there is no default deserializer defined for type {typeof(TKey).Name}.");
                }
                this.keyDeserializer = (IDeserializer<TKey>)deserializer;
            }

            if (valueDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TValue), out object deserializer))
                {
                    throw new ArgumentNullException(
                        $"Value deserializer not specified and there is no default deserializer defined for type {typeof(TValue).Name}.");
                }
                this.valueDeserializer = (IDeserializer<TValue>)deserializer;
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer" />
        /// </summary>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            IDeserializer<TKey> keyDeserializer,
            IAsyncDeserializer<TValue> taskValueDeserializer
        ) : base(config)
        {
            this.keyDeserializer = keyDeserializer;
            this.taskValueDeserializer = taskValueDeserializer;

            if (keyDeserializer == null)
            {
                throw new ArgumentNullException("Key deserializer must be specified.");
            }

            if (taskValueDeserializer == null)
            {
                throw new ArgumentNullException("Value deserializer must be specified.");
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer" />
        /// </summary>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            IAsyncDeserializer<TKey> taskKeyDeserializer,
            IDeserializer<TValue> valueDeserializer
        ) : base(config)
        {
            this.taskKeyDeserializer = taskKeyDeserializer;
            this.valueDeserializer = valueDeserializer;

            if (this.taskKeyDeserializer == null)
            {
                throw new ArgumentNullException("Key deserializer must be specified.");
            }

            if (this.valueDeserializer == null)
            {
                throw new ArgumentNullException("Value deserializer must be specified.");
            }
        }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer" />
        /// </summary>
        public Consumer(
            IEnumerable<KeyValuePair<string, string>> config,
            IAsyncDeserializer<TKey> taskKeyDeserializer,
            IAsyncDeserializer<TValue> taskValueDeserializer
        ) : base(config)
        {
            this.taskKeyDeserializer = taskKeyDeserializer;
            this.taskValueDeserializer = taskValueDeserializer;

            if (this.taskKeyDeserializer == null)
            {
                throw new ArgumentNullException("Key deserializer must be specified.");
            }

            if (this.taskValueDeserializer == null)
            {
                throw new ArgumentNullException("Value deserializer must be specified.");
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
                : taskKeyDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Key), rawResult.Key == null, true, rawResult.Message, rawResult.TopicPartition)
                    .ConfigureAwait(continueOnCapturedContext: false)
                    .GetAwaiter()
                    .GetResult();

            TValue val = valueDeserializer != null
                ? valueDeserializer.Deserialize(rawResult.Value, rawResult.Value == null, false, rawResult.Message, rawResult.TopicPartition)
                : taskValueDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(rawResult.Value), rawResult == null, false, rawResult.Message, rawResult.TopicPartition)
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
    }


    /// <summary>
    ///     Implements a high-level Apache Kafka consumer.
    /// </summary>
    public class Consumer : ConsumerBase, IConsumer
    {
        /// <summary>
        ///     Creates a new <see cref="Confluent.Kafka.Consumer" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        public Consumer(IEnumerable<KeyValuePair<string, string>> config) : base(config) {}

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
    }
}
