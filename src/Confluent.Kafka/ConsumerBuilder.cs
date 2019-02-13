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
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder class for <see cref="Consumer{TKey,TValue}" /> instances.
    /// </summary>
    public class ConsumerBuilder<TKey, TValue>
    {
        /// <summary>
        ///     The config dictionary.
        /// </summary>
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        /// <summary>
        ///     The configured error handler.
        /// </summary>
        internal protected Action<Consumer<TKey, TValue>, Error> ErrorHandler { get; set; }

        /// <summary>
        ///     The configured log handler.
        /// </summary>
        internal protected Action<Consumer<TKey, TValue>, LogMessage> LogHandler { get; set; }

        /// <summary>
        ///     The configured statistics handler.
        /// </summary>
        internal protected Action<Consumer<TKey, TValue>, string> StatisticsHandler { get; set; }

        /// <summary>
        ///     The configured key deserializer.
        /// </summary>
        internal protected IDeserializer<TKey> KeyDeserializer { get; set; }

        /// <summary>
        ///     The configured value deserializer.
        /// </summary>
        internal protected IDeserializer<TValue> ValueDeserializer { get; set; }

        /// <summary>
        ///     The configured async key deserializer.
        /// </summary>
        internal protected IAsyncDeserializer<TKey> AsyncKeyDeserializer { get; set; }

        /// <summary>
        ///     The configured async value deserializer.
        /// </summary>
        internal protected IAsyncDeserializer<TValue> AsyncValueDeserializer { get; set; }

        /// <summary>
        ///     The configured rebalance handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, RebalanceEvent> RebalanceHandler { get; set; }

        /// <summary>
        ///     The configured offsets committed handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, CommittedOffsets> OffsetsCommittedHandler { get; set; }

        internal Consumer<TKey,TValue>.Config ConstructBaseConfig(Consumer<TKey, TValue> consumer)
        {
            return new Consumer<TKey,TValue>.Config
            {
                config = Config,
                errorHandler = this.ErrorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.ErrorHandler(consumer, error),
                logHandler = this.LogHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.LogHandler(consumer, logMessage),
                statisticsHandler = this.StatisticsHandler == null
                    ? default(Action<string>)
                    : stats => this.StatisticsHandler(consumer, stats),
                offsetsCommittedHandler = this.OffsetsCommittedHandler == null
                    ? default(Action<CommittedOffsets>)
                    : offsets => this.OffsetsCommittedHandler(consumer, offsets),
                rebalanceHandler = this.RebalanceHandler == null
                    ? default(Action<RebalanceEvent>)
                    : rebalanceEvent => this.RebalanceHandler(consumer, rebalanceEvent)
            };
        }

        /// <summary>
        ///     Initialize a new ConsumerBuilder instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </param>
        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }

        /// <summary>
        ///     Set the handler to call on statistics events. Statistics 
        ///     are provided as a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Executes as a side-effect of the Consume method (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetStatisticsHandler(
            Action<Consumer<TKey, TValue>, string> statisticsHandler)
        {
            if (this.StatisticsHandler != null)
            {
                throw new InvalidOperationException("Statistics handler may not be specified more than once.");
            }
            this.StatisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal. Non-fatal errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consume method (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetErrorHandler(
            Action<Consumer<TKey, TValue>, Error> errorHandler)
        {
            if (this.ErrorHandler != null)
            {
                throw new InvalidOperationException("Error handler may not be specified more than once.");
            }
            this.ErrorHandler = errorHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call when there is information available
        ///     to be logged. If not specified, a default callback that writes
        ///     to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts
        ///     using the 'debug' configuration property.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetLogHandler(
            Action<Consumer<TKey, TValue>, LogMessage> logHandler)
        {
            if (this.LogHandler != null)
            {
                throw new InvalidOperationException("Log handler may not be specified more than once.");
            }
            this.LogHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize keys.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IDeserializer<TKey> deserializer)
        {
            if (this.KeyDeserializer != null || this.AsyncKeyDeserializer != null)
            {
                throw new InvalidOperationException("Key deserializer may not be specified more than once.");
            }
            this.KeyDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize values.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)
        {
            if (this.ValueDeserializer != null || this.AsyncValueDeserializer != null)
            {
                throw new InvalidOperationException("Value deserializer may not be specified more than once.");
            }
            this.ValueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the async deserializer to use to deserialize keys.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IAsyncDeserializer<TKey> deserializer)
        {
            if (this.KeyDeserializer != null || this.AsyncKeyDeserializer != null)
            {
                throw new InvalidOperationException("Key deserializer may not be specified more than once.");
            }
            this.AsyncKeyDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the async deserializer to use to deserialize values.
        /// </summary>
        public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IAsyncDeserializer<TValue> deserializer)
        {
            if (this.ValueDeserializer != null || this.AsyncValueDeserializer != null)
            {
                throw new InvalidOperationException("Value deserializer may not be specified more than once.");
            }
            this.AsyncValueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the consumer group rebalance handler. Except for the initial partition assignment
        ///     and final assignment revocation, a group rebalance will cause this handler to be
        ///     called twice. The first time to advise that the current assignment has been revoked
        ///     and the second time to advise of the new assignment.
        /// 
        ///     If you do not call an Assign or Unassign method in this handler, or do not specify
        ///     a handler, partitions will be assigned (or unassigned) automatically, matching the
        ///     assignment dictated by the consumer group. This default behavior will not occur if
        ///     you call Assign yourself in the handler. The set of partitions you assign to is not
        ///     required to match the assignment provided by the consumer group, but typically will.
        ///     Note: you may call Assign or Unassign at most once in your rebalance handler.
        /// 
        ///     Consumption will resume from the last committed offset for each partition (unless an
        ///     offset is manually specified in a call to Assign), or if there is no committed
        ///     offset, in accordance with the `auto.offset.reset` configuration property.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume call (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetRebalanceHandler(Action<IConsumer<TKey, TValue>, RebalanceEvent> rebalanceHandler)
        {
            if (this.RebalanceHandler != null)
            {
                throw new InvalidOperationException("Rebalance handler may not be specified more than once.");
            }
            this.RebalanceHandler = rebalanceHandler;
            return this;
        }

        /// <summary>
        ///     A handler that is called to report the result of (automatic) offset 
        ///     commits. It is not called as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetOffsetsCommittedHandler(Action<IConsumer<TKey, TValue>, CommittedOffsets> offsetsCommittedHandler)
        {
            if (this.OffsetsCommittedHandler != null)
            {
                throw new InvalidOperationException("Offsets committed handler may not be specified more than once.");
            }
            this.OffsetsCommittedHandler = offsetsCommittedHandler;
            return this;
        }

        /// <summary>
        ///     Build a new Consumer instance.
        /// </summary>
        public virtual IConsumer<TKey, TValue> Build()
        {
            return new Consumer<TKey, TValue>(this);
        }
    }
}
