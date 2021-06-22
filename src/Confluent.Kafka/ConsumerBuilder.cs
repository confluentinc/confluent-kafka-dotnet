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


namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder class for <see cref="IConsumer{TKey,TValue}" />.
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
        internal protected Action<IConsumer<TKey, TValue>, Error> ErrorHandler { get; set; }

        /// <summary>
        ///     The configured log handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, LogMessage> LogHandler { get; set; }

        /// <summary>
        ///     The configured statistics handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, string> StatisticsHandler { get; set; }

        /// <summary>
        ///     The configured OAuthBearer Token Refresh handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, string> OAuthBearerTokenRefreshHandler { get; set; }

        /// <summary>
        ///     The configured key deserializer.
        /// </summary>
        internal protected IDeserializer<TKey> KeyDeserializer { get; set; }

        /// <summary>
        ///     The configured value deserializer.
        /// </summary>
        internal protected IDeserializer<TValue> ValueDeserializer { get; set; }

        /// <summary>
        ///     The configured partitions assigned handler.
        /// </summary>
        internal protected Func<IConsumer<TKey, TValue>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> PartitionsAssignedHandler { get; set; }

        /// <summary>
        ///     The configured partitions revoked handler.
        /// </summary>
        internal protected Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> PartitionsRevokedHandler { get; set; }

        /// <summary>
        ///     Whether or not the user configured either PartitionsRevokedHandler or PartitionsLostHandler
        ///     as a Func (as opposed to an Action).
        /// </summary>
        internal protected bool RevokedOrLostHandlerIsFunc = false;

        /// <summary>
        ///     The configured partitions lost handler.
        /// </summary>
        internal protected Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> PartitionsLostHandler { get; set; }

        /// <summary>
        ///     The configured offsets committed handler.
        /// </summary>
        internal protected Action<IConsumer<TKey, TValue>, CommittedOffsets> OffsetsCommittedHandler { get; set; }

        internal Consumer<TKey,TValue>.Config ConstructBaseConfig(Consumer<TKey, TValue> consumer)
        {
            return new Consumer<TKey, TValue>.Config
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
                oAuthBearerTokenRefreshHandler = this.OAuthBearerTokenRefreshHandler == null
                    ? default(Action<string>)
                    : oAuthBearerConfig => this.OAuthBearerTokenRefreshHandler(consumer, oAuthBearerConfig),
                partitionsAssignedHandler = this.PartitionsAssignedHandler == null
                    ? default(Func<List<TopicPartition>, IEnumerable<TopicPartitionOffset>>)
                    : partitions => this.PartitionsAssignedHandler(consumer, partitions),
                partitionsRevokedHandler = this.PartitionsRevokedHandler == null
                    ? default(Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>>)
                    : partitions => this.PartitionsRevokedHandler(consumer, partitions),
                partitionsLostHandler = this.PartitionsLostHandler == null
                    ? default(Func<List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>>)
                    : partitions => this.PartitionsLostHandler(consumer, partitions),
                revokedOrLostHandlerIsFunc = this.RevokedOrLostHandlerIsFunc
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
        ///     using the StatisticsIntervalMs configuration property
        ///     (disabled by default).
        ///
        ///     Executes as a side-effect of the Consume method (on the same
        ///     thread).
        ///
        ///     Exceptions: Any exception thrown by your statistics handler
        ///     will be wrapped in a ConsumeException with ErrorCode
        ///     ErrorCode.Local_Application and thrown by the initiating call
        ///     to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetStatisticsHandler(
            Action<IConsumer<TKey, TValue>, string> statisticsHandler)
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
        ///
        ///     Exceptions: Any exception thrown by your error handler will be silently
        ///     ignored.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetErrorHandler(
            Action<IConsumer<TKey, TValue>, Error> errorHandler)
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
        ///     using the 'Debug' configuration property.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        ///
        ///     Exceptions: Any exception thrown by your log handler will be
        ///     silently ignored.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetLogHandler(
            Action<IConsumer<TKey, TValue>, LogMessage> logHandler)
        {
            if (this.LogHandler != null)
            {
                throw new InvalidOperationException("Log handler may not be specified more than once.");
            }
            this.LogHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Set SASL/OAUTHBEARER token refresh callback in provided
        ///     conf object. The SASL/OAUTHBEARER token refresh callback
        ///     is triggered via <see cref="IConsumer{TKey,TValue}.Consume(int)"/>
        ///     (or any of its overloads) whenever OAUTHBEARER is the SASL
        ///     mechanism and a token needs to be retrieved, typically
        ///     based on the configuration defined in
        ///     sasl.oauthbearer.config. The callback should invoke
        ///     <see cref="ClientExtensions.OAuthBearerSetToken"/>
        ///     or <see cref="ClientExtensions.OAuthBearerSetTokenFailure"/>
        ///     to indicate success or failure, respectively.
        ///
        ///     An unsecured JWT refresh handler is provided by librdkafka
        ///     for development and testing purposes, it is enabled by
        ///     setting the enable.sasl.oauthbearer.unsecure.jwt property
        ///     to true and is mutually exclusive to using a refresh callback.
        /// </summary>
        /// <param name="oAuthBearerTokenRefreshHandler">
        ///     the callback to set; callback function arguments:
        ///     IConsumer - instance of the consumer which should be used to
        ///     set token or token failure string - Value of configuration
        ///     property sasl.oauthbearer.config
        /// </param>
        public ConsumerBuilder<TKey, TValue> SetOAuthBearerTokenRefreshHandler(Action<IConsumer<TKey, TValue>, string> oAuthBearerTokenRefreshHandler)
        {
            if (this.OAuthBearerTokenRefreshHandler != null)
            {
                throw new InvalidOperationException("OAuthBearer token refresh handler may not be specified more than once.");
            }
            this.OAuthBearerTokenRefreshHandler = oAuthBearerTokenRefreshHandler;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key deserializer throws an exception, this will be
        ///     wrapped in a ConsumeException with ErrorCode
        ///     Local_KeyDeserialization and thrown by the initiating call to
        ///     Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IDeserializer<TKey> deserializer)
        {
            if (this.KeyDeserializer != null)
            {
                throw new InvalidOperationException("Key deserializer may not be specified more than once.");
            }
            this.KeyDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Set the deserializer to use to deserialize values.
        /// </summary>
        /// <remarks>
        ///     If your value deserializer throws an exception, this will be
        ///     wrapped in a ConsumeException with ErrorCode
        ///     Local_ValueDeserialization and thrown by the initiating call to
        ///     Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)
        {
            if (this.ValueDeserializer != null)
            {
                throw new InvalidOperationException("Value deserializer may not be specified more than once.");
            }
            this.ValueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Specify a handler that will be called when a new consumer group partition assignment has
        ///     been received by this consumer.
        ///
        ///     The actual partitions to consume from and start offsets are specified by the return value
        ///     of the handler. Partition offsets may be a specific offset, or special value (Beginning, End
        ///     or Unset). If Unset, consumption will resume from the last committed offset for each
        ///     partition, or if there is no committed offset, in accordance with the `auto.offset.reset`
        ///     configuration property.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     The set of partitions returned from your handler may differ from that provided by the
        ///     group (though they should typically be the same). These partitions are the
        ///     entire set of partitions to consume from. There will be exactly one call to the
        ///     partitions revoked or partitions lost handler (if they have been set using
        ///     SetPartitionsRevokedHandler / SetPartitionsLostHandler) corresponding to every call to
        ///     this handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     The set of partitions returned from your handler must match that provided by the
        ///     group. These partitions are an incremental assignment - are in addition to those
        ///     already being consumed from.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///     
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions assigned handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandler)
        {
            if (this.PartitionsAssignedHandler != null)
            {
                throw new InvalidOperationException("The partitions assigned handler may not be specified more than once.");
            }

            this.PartitionsAssignedHandler = partitionsAssignedHandler;

            return this;
        }

        /// <summary>
        ///     Specify a handler that will be called when a new consumer group partition assignment has
        ///     been received by this consumer.
        ///
        ///     Following execution of the handler, consumption will resume from the last committed offset
        ///     for each partition, or if there is no committed offset, in accordance with the
        ///     `auto.offset.reset` configuration property.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     Partitions passed to the handler represent the entire set of partitions to consume from.
        ///     There will be exactly one call to the partitions revoked or partitions lost handler (if
        ///     they have been set using SetPartitionsRevokedHandler / SetPartitionsLostHandler)
        ///     corresponding to every call to this handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     Partitions passed to the handler are an incremental assignment - are in addition to those
        ///     already being consumed from.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions assigned handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionAssignmentHandler)
        {
            if (this.PartitionsAssignedHandler != null)
            {
                throw new InvalidOperationException("The partitions assigned handler may not be specified more than once.");
            }

            this.PartitionsAssignedHandler = (IConsumer<TKey, TValue> consumer, List<TopicPartition> partitions) =>
            {
                partitionAssignmentHandler(consumer, partitions);
                return partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Unset)).ToList();
            };

            return this;
        }


        /// <summary>
        ///     Specify a handler that will be called immediately prior to the consumer's current assignment
        ///     being revoked, allowing the application to take action (e.g. offsets committed to a custom
        ///     store) before the consumer gives up ownership of the partitions. The Func partitions revoked
        ///     handler variant is not supported in the incremental rebalancing (COOPERATIVE) case.
        ///
        ///     The value returned from your handler specifies the partitions/offsets the consumer should
        ///     be assigned to read from following completion of this method (most typically empty). This
        ///     partitions revoked handler variant may not be specified when incremental rebalancing is in use
        ///     - in that case, the set of partitions the consumer is reading from may never deviate from
        ///     the set that it has been assigned by the group.
        ///
        ///     The second parameter provided to the handler provides the set of partitions the consumer is
        ///     currently assigned to, and the current position of the consumer on each of these partitions.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume/Close/Dispose call (on the same thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped in a
        ///     ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the initiating call
        ///     to Consume/Close.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsRevokedHandler)
        {
            if (this.PartitionsRevokedHandler != null)
            {
                throw new InvalidOperationException("The partitions revoked handler may not be specified more than once.");
            }

            this.PartitionsRevokedHandler = partitionsRevokedHandler;
            this.RevokedOrLostHandlerIsFunc = true;

            return this;
        }

        /// <summary>
        ///     Specify a handler that will be called immediately prior to partitions being revoked
        ///     from the consumer's current assignment, allowing the application to take action
        ///     (e.g. commit offsets to a custom store) before the consumer gives up ownership of
        ///     the partitions.
        ///
        ///     Kafka supports two rebalance protocols: EAGER (range and roundrobin assignors) and
        ///     COOPERATIVE (incremental) (cooperative-sticky assignor). Use the PartitionAssignmentStrategy
        ///     configuration property to specify which assignor to use.
        ///
        ///     ## EAGER Rebalancing (range, roundrobin)
        ///
        ///     The second parameter provides the entire set of partitions the consumer is currently
        ///     assigned to, and the current position of the consumer on each of these partitions.
        ///     The consumer will stop consuming from all partitions following execution of this
        ///     handler.
        ///
        ///     ## COOPERATIVE (Incremental) Rebalancing
        ///
        ///     The second parameter provides the subset of the partitions assigned to the consumer
        ///     which are being revoked, and the current position of the consumer on each of these
        ///     partitions. The consumer will stop consuming from this set of partitions following
        ///     execution of this handler, and continue reading from any remaining partitions.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsRevokedHandler)
        {
            if (this.PartitionsRevokedHandler != null)
            {
                throw new InvalidOperationException("The partitions revoked handler may not be specified more than once.");
            }

            this.PartitionsRevokedHandler = (IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions) =>
            {
                partitionsRevokedHandler(consumer, partitions);
                return new List<TopicPartitionOffset>();
            };

            return this;
        }

        /// <summary>
        ///     Specify a handler that will be called when the consumer detects that it has lost ownership
        ///     of its partition assignment (fallen out of the group). The application should not commit
        ///     offsets in this case, since the partitions will likely be owned by other consumers in the
        ///     group (offset commits to Kafka will likely fail).
        ///
        ///     The value returned from your handler specifies the partitions/offsets the consumer should
        ///     be assigned to read from following completion of this method (most typically empty). This
        ///     partitions lost handler variant may not be specified when incremental rebalancing is in use
        ///     - in that case, the set of partitions the consumer is reading from may never deviate from
        ///     the set that it has been assigned by the group.
        ///
        ///     The second parameter provided to the handler provides the set of all partitions the consumer
        ///     is currently assigned to, and the current position of the consumer on each of these partitions.
        ///     Following completion of this handler, the consumer will stop consuming from all partitions.
        ///
        ///     If this handler is not specified, the partitions revoked handler (if specified) will be called
        ///     instead if partitions are lost.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsLostHandler(
            Func<IConsumer<TKey, TValue>, List<TopicPartitionOffset>, IEnumerable<TopicPartitionOffset>> partitionsLostHandler)
        {
            if (this.PartitionsLostHandler != null)
            {
                throw new InvalidOperationException("The partitions lost handler may not be specified more than once.");
            }

            this.PartitionsLostHandler = partitionsLostHandler;
            this.RevokedOrLostHandlerIsFunc = true;

            return this;
        }

        /// <summary>
        ///     Specify a handler that will be called when the consumer detects that it has lost ownership
        ///     of its partition assignment (fallen out of the group). The application should not commit
        ///     offsets in this case, since the partitions will likely be owned by other consumers in the
        ///     group (offset commits to Kafka will likely fail).
        ///
        ///     The second parameter provided to the handler provides the set of all partitions the consumer
        ///     is currently assigned to, and the current position of the consumer on each of these partitions.
        ///
        ///     If this handler is not specified, the partitions revoked handler (if specified) will be called
        ///     instead if partitions are lost.
        /// </summary>
        /// <remarks>
        ///     May execute as a side-effect of the Consumer.Consume/Close/Dispose call (on the same
        ///     thread).
        ///
        ///     (Incremental)Assign/Unassign must not be called in the handler.
        ///
        ///     Exceptions: Any exception thrown by your partitions revoked handler will be wrapped
        ///     in a ConsumeException with ErrorCode ErrorCode.Local_Application and thrown by the
        ///     initiating call to Consume.
        /// </remarks>
        public ConsumerBuilder<TKey, TValue> SetPartitionsLostHandler(
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionsLostHandler)
        {
            if (this.PartitionsLostHandler != null)
            {
                throw new InvalidOperationException("The partitions lost handler may not be specified more than once.");
            }

            this.PartitionsLostHandler = (IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions) =>
            {
                partitionsLostHandler(consumer, partitions);
                return new List<TopicPartitionOffset>();
            };

            return this;
        }

        /// <summary>
        ///     A handler that is called to report the result of (automatic) offset 
        ///     commits. It is not called as a result of the use of the Commit method.
        /// </summary>
        /// <remarks>
        ///     Executes as a side-effect of the Consumer.Consume call (on the same thread).
        ///
        ///     Exceptions: Any exception thrown by your offsets committed handler
        ///     will be wrapped in a ConsumeException with ErrorCode
        ///     ErrorCode.Local_Application and thrown by the initiating call to Consume/Close.
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
        ///     Build a new IConsumer implementation instance.
        /// </summary>
        public virtual IConsumer<TKey, TValue> Build()
        {
            return new Consumer<TKey, TValue>(this);
        }
    }
}
