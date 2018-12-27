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
    ///     A builder class for <see cref="Producer" /> instances.
    /// </summary>
    public class ProducerBuilder
    {
        internal IEnumerable<KeyValuePair<string, string>> config;
        internal Action<Producer, Error> errorHandler;
        internal Action<Producer, LogMessage> logHandler;
        internal Action<Producer, string> statisticsHandler;

        internal ProducerBase.Config ConstructBaseConfig(Producer producer)
        {
            return new ProducerBase.Config
            {
                config = config,
                errorHandler = this.errorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.errorHandler(producer, error),
                logHandler = this.logHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.logHandler(producer, logMessage),
                statsHandler = this.statisticsHandler == null
                    ? default(Action<string>)
                    : stats => this.statisticsHandler(producer, stats)
            };
        }

        /// <summary>
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </summary>
        public ProducerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.config = config;
        }

        /// <summary>
        ///     Refer to <see cref="ProducerBuilder{TKey,TValue}.SetStatisticsHandler(Action{Producer{TKey, TValue}, string})" />.
        /// </summary>
        public ProducerBuilder SetStatisticsHandler(Action<Producer, string> statisticsHandler)
        {
            this.statisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ProducerBuilder{TKey,TValue}.SetErrorHandler(Action{Producer{TKey, TValue}, Error})" />.
        /// </summary>
        public ProducerBuilder SetErrorHandler(Action<Producer, Error> errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ProducerBuilder{TKey,TValue}.SetLogHandler(Action{Producer{TKey, TValue}, LogMessage})" />.
        /// </summary>
        public ProducerBuilder SetLogHandler(Action<Producer, LogMessage> logHandler)
        {
            this.logHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ProducerBuilder{TKey,TValue}.Build()" />.
        /// </summary>
        public Producer Build()
        {
            return new Producer(this);
        }
    }


    /// <summary>
    ///     A builder class for <see cref="Producer{TKey, TValue}" /> instances.
    /// </summary>
    public class ProducerBuilder<TKey, TValue>
    {
        internal IEnumerable<KeyValuePair<string, string>> config;
        internal Action<Producer<TKey, TValue>, Error> errorHandler;
        internal Action<Producer<TKey, TValue>, LogMessage> logHandler;
        internal Action<Producer<TKey, TValue>, string> statisticsHandler;
        
        internal ISerializer<TKey> keySerializer;
        internal ISerializer<TValue> valueSerializer;
        internal IAsyncSerializer<TKey> asyncKeySerializer;
        internal IAsyncSerializer<TValue> asyncValueSerializer;

        internal ProducerBase.Config ConstructBaseConfig(Producer<TKey, TValue> producer)
        {
            return new ProducerBase.Config
            {
                config = config,
                errorHandler = this.errorHandler == null
                    ? default(Action<Error>) // using default(...) rather than null (== default(...)) so types can be inferred.
                    : error => this.errorHandler(producer, error),
                logHandler = this.logHandler == null
                    ? default(Action<LogMessage>)
                    : logMessage => this.logHandler(producer, logMessage),
                statsHandler = this.statisticsHandler == null
                    ? default(Action<string>)
                    : stats => this.statisticsHandler(producer, stats)
            };
        }

        /// <summary>
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and 'group.id' must be
        ///     specified.
        /// </summary>
        public ProducerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.config = config;
        }

        /// <summary>
        ///     Set the handler to call on librdkafka statistics events. Statistics are provided as a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetStatisticsHandler(Action<Producer<TKey, TValue>, string> statisticsHandler)
        {
            this.statisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal - such errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes on the poll thread (by default, a background thread managed by
        ///     the producer).
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetErrorHandler(Action<Producer<TKey, TValue>, Error> errorHandler)
        {
            this.errorHandler = errorHandler;
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
        ///     using the 'debug' configuration property. The 'log_level'
        ///     configuration property is also relevant, however logging is
        ///     verbose by default given a debug context has been specified,
        ///     so you typically shouldn't adjust this value.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        /// </remarks>
        public ProducerBuilder<TKey, TValue> SetLogHandler(Action<Producer<TKey, TValue>, LogMessage> logHandler)
        {
            this.logHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer)
        {
            this.keySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
        {
            this.valueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer)
        {
            this.asyncKeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public ProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            this.asyncValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     Build a new Producer instance.
        /// </summary>
        public Producer<TKey, TValue> Build()
        {
            return new Producer<TKey, TValue>(this);
        }
    }
}
