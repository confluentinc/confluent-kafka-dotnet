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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Calculate a partition number given a <paramref name="partitionCount" />
    ///     and serialized <paramref name="keyData" />. The <paramref name="topic" />
    ///     is also provided, but is typically not used.
    /// </summary>
    /// <remarks>
    ///     A partioner instance may be called in any thread at any time and
    ///     may be called multiple times for the same message/key.
    ///
    ///     A partitioner:
    ///     - MUST NOT call any method on the producer instance.
    ///     - MUST NOT block or execute for prolonged periods of time.
    ///     - MUST return a value between 0 and partitionCnt-1.
    ///     - MUST NOT throw any exception.
    /// </remarks>
    /// <param name="topic">
    ///     The topic.
    /// </param>
    /// <param name="partitionCount">
    ///     The number of partitions in <paramref name="topic" />.
    /// </param>
    /// <param name="keyData">
    ///     The serialized key data.
    /// </param>
    /// <param name="keyIsNull">
    ///     Whether or not the key is null (distinguishes the null and empty case).
    /// </param>
    /// <returns>
    ///     The calculated <seealso cref="Confluent.Kafka.Partition"/>, possibly
    ///     <seealso cref="Confluent.Kafka.Partition.Any"/>.
    /// </returns>
    public delegate Partition PartitionerDelegate(string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull);


    /// <summary>
    ///     A builder class for <see cref="IProducer{TKey,TValue}" />.
    /// </summary>
    public class ProducerBuilder<TKey, TValue>
    {
        /// <summary>
        ///     The config dictionary.
        /// </summary>
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        /// <summary>
        ///     The configured error handler.
        /// </summary>
        internal protected Action<IProducer<TKey, TValue>, Error> ErrorHandler { get; set; }

        /// <summary>
        ///     The configured log handler.
        /// </summary>
        internal protected Action<IProducer<TKey, TValue>, LogMessage> LogHandler { get; set; }

        /// <summary>
        ///     The configured statistics handler.
        /// </summary>
        internal protected Action<IProducer<TKey, TValue>, string> StatisticsHandler { get; set; }

        /// <summary>
        ///     The configured OAuthBearer Token Refresh handler.
        /// </summary>
        internal protected Action<IProducer<TKey, TValue>, string> OAuthBearerTokenRefreshHandler { get; set; }

        /// <summary>        
        ///     The per-topic custom partitioners.
        /// </summary>
        internal protected Dictionary<string, PartitionerDelegate> Partitioners { get; set; } = new Dictionary<string, PartitionerDelegate>();

        /// <summary>
        ///     The default custom partitioner.
        /// </summary>
        internal protected PartitionerDelegate DefaultPartitioner { get; set; } = null;

        /// <summary>
        ///     The configured key serializer.
        /// </summary>
        internal protected ISerializer<TKey> KeySerializer { get; set; }

        /// <summary>
        ///     The configured value serializer.
        /// </summary>
        internal protected ISerializer<TValue> ValueSerializer { get; set; }

        /// <summary>
        ///     The configured async key serializer.
        /// </summary>
        internal protected IAsyncSerializer<TKey> AsyncKeySerializer { get; set; }

        /// <summary>
        ///     The configured async value serializer.
        /// </summary>
        internal protected IAsyncSerializer<TValue> AsyncValueSerializer { get; set; }

        internal Producer<TKey,TValue>.Config ConstructBaseConfig(Producer<TKey, TValue> producer)
        {
            return new Producer<TKey, TValue>.Config
            {
                config = Config,
                errorHandler = this.ErrorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.ErrorHandler(producer, error),
                logHandler = this.LogHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.LogHandler(producer, logMessage),
                statisticsHandler = this.StatisticsHandler == null
                    ? default(Action<string>)
                    : stats => this.StatisticsHandler(producer, stats),
                oAuthBearerTokenRefreshHandler = this.OAuthBearerTokenRefreshHandler == null
                    ? default(Action<string>)
                    : oAuthBearerConfig => this.OAuthBearerTokenRefreshHandler(producer, oAuthBearerConfig),
                partitioners = this.Partitioners,
                defaultPartitioner = this.DefaultPartitioner,
            };
        }

        /// <summary>
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.
        /// </summary>
        public ProducerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }

        /// <summary>
        ///     Set the handler to call on statistics events. Statistics are provided as
        ///     a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the StatisticsIntervalMs configuration property
        ///     (disabled by default).
        ///
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        ///
        ///     Exceptions: Any exception thrown by your statistics handler
        ///     will be devivered to your error handler, if set, else they will be
        ///     silently ignored.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetStatisticsHandler(Action<IProducer<TKey, TValue>, string> statisticsHandler)
        {
            if (this.StatisticsHandler != null)
            {
                throw new InvalidOperationException("Statistics handler may not be specified more than once.");
            }
            this.StatisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Set a custom partitioner to use when producing messages to
        ///     <paramref name="topic" />.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetPartitioner(string topic, PartitionerDelegate partitioner)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Topic must not be null or empty");
            }
            if (this.Partitioners.ContainsKey(topic))
            {
                throw new ArgumentException($"Custom partitioner for {topic} already specified");
            }
            this.Partitioners.Add(topic, partitioner);
            return this;
        }

        /// <summary>
        ///     Set a custom partitioner that will be used for all topics
        ///     except those for which a partitioner has been explicitly configured.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetDefaultPartitioner(PartitionerDelegate partitioner)
        {
            if (this.DefaultPartitioner != null)
            {
                throw new ArgumentException("Default custom partitioner may only be specified once");
            }
            this.DefaultPartitioner = partitioner;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal. Non-fatal errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        ///
        ///     Exceptions: Any exception thrown by your error handler will be silently
        ///     ignored.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetErrorHandler(Action<IProducer<TKey, TValue>, Error> errorHandler)
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
        ///     using the Debug configuration property.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        ///
        ///     Exceptions: Any exception thrown by your log handler will be
        ///     silently ignored.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetLogHandler(Action<IProducer<TKey, TValue>, LogMessage> logHandler)
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
        ///     is triggered via <see cref="IProducer{TKey,TValue}.Poll"/>
        ///     whenever OAUTHBEARER is the SASL mechanism and a token
        ///     needs to be retrieved, typically based on the configuration
        ///     defined in sasl.oauthbearer.config. The callback should
        ///     invoke <see cref="ClientExtensions.OAuthBearerSetToken"/>
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
        public ProducerBuilder<TKey, TValue> SetOAuthBearerTokenRefreshHandler(Action<IProducer<TKey, TValue>, string> oAuthBearerTokenRefreshHandler)
        {
            if (this.OAuthBearerTokenRefreshHandler != null)
            {
                throw new InvalidOperationException("OAuthBearer token refresh handler may not be specified more than once.");
            }
            this.OAuthBearerTokenRefreshHandler = oAuthBearerTokenRefreshHandler;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_KeySerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer)
        {
            if (this.KeySerializer != null || this.AsyncKeySerializer != null)
            {
                throw new InvalidOperationException("Key serializer may not be specified more than once.");
            }
            this.KeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        /// <remarks>
        ///     If your value serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_ValueSerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
        {
            if (this.ValueSerializer != null || this.AsyncValueSerializer != null)
            {
                throw new InvalidOperationException("Value serializer may not be specified more than once.");
            }
            this.ValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        /// <remarks>
        ///     If your key serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_KeySerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer)
        {
            if (this.KeySerializer != null || this.AsyncKeySerializer != null)
            {
                throw new InvalidOperationException("Key serializer may not be specified more than once.");
            }
            this.AsyncKeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        /// <remarks>
        ///     If your value serializer throws an exception, this will be
        ///     wrapped in a ProduceException with ErrorCode
        ///     Local_ValueSerialization and thrown by the initiating call to
        ///     Produce or ProduceAsync.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            if (this.ValueSerializer != null || this.AsyncValueSerializer != null)
            {
                throw new InvalidOperationException("Value serializer may not be specified more than once.");
            }
            this.AsyncValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     Build a new IProducer implementation instance.
        /// </summary>
        public virtual IProducer<TKey, TValue> Build()
        {
            return new Producer<TKey, TValue>(this);
        }
    }
}
