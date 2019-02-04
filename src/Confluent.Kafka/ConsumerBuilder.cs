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
    ///     A builder class for <see cref="Consumer" /> instances.
    /// </summary>
    public class ConsumerBuilder
    {
        /// <summary>
        ///     The config dictionary.
        /// </summary>
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        /// <summary>
        ///     The configured error handler.
        /// </summary>
        internal protected Action<Consumer, Error> ErrorHandler { get; set; }

        /// <summary>
        ///     The configured log handler.
        /// </summary>
        internal protected Action<Consumer, LogMessage> LogHandler { get; set; }

        /// <summary>
        ///     The configured statistics handler.
        /// </summary>
        internal protected Action<Consumer, string> StatisticsHandler { get; set; }


        internal ConsumerBase.Config ConstructBaseConfig(Consumer consumer)
        {
            return new ConsumerBase.Config
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
                    : stats => this.StatisticsHandler(consumer, stats)
            };
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.ConsumerBuilder(IEnumerable{KeyValuePair{string, string}})" />.
        /// </summary>
        public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetStatisticsHandler(Action{Consumer{TKey,TValue}, string})" />.
        /// </summary>
        public ConsumerBuilder SetStatisticsHandler(Action<Consumer, string> statisticsHandler)
        {
            this.StatisticsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetErrorHandler(Action{Consumer{TKey,TValue}, Error})" />.
        /// </summary>
        public ConsumerBuilder SetErrorHandler(Action<Consumer, Error> errorHandler)
        {
            this.ErrorHandler = errorHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.SetLogHandler(Action{Consumer{TKey,TValue}, LogMessage})" />.
        /// </summary>
        public ConsumerBuilder SetLogHandler(Action<Consumer, LogMessage> logHandler)
        {
            this.LogHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Refer to <see cref="ConsumerBuilder{TKey,TValue}.Build" />.
        /// </summary>
        public virtual Consumer Build()
        {
            return new Consumer(this);
        }
    }


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

        internal ConsumerBase.Config ConstructBaseConfig(Consumer<TKey, TValue> consumer)
        {
            return new ConsumerBase.Config
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
                    : stats => this.StatisticsHandler(consumer, stats)
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
                throw new ArgumentException("Statistics handler may not be specified more than once.");
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
                throw new ArgumentException("Error handler may not be specified more than once.");
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
                throw new ArgumentException("Log handler may not be specified more than once.");
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
                throw new ArgumentException("Key deserializer may not be specified more than once.");
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
                throw new ArgumentException("Value deserializer may not be specified more than once.");
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
                throw new ArgumentException("Key deserializer may not be specified more than once.");
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
                throw new ArgumentException("Value deserializer may not be specified more than once.");
            }
            this.AsyncValueDeserializer = deserializer;
            return this;
        }

        /// <summary>
        ///     Build a new Consumer instance.
        /// </summary>
        public virtual Consumer<TKey, TValue> Build()
        {
            return new Consumer<TKey, TValue>(this);
        }
    }
}
