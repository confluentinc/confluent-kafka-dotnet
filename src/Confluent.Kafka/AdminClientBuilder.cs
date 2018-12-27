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
    ///     A builder for <see cref="AdminClient" /> instances.
    /// </summary>
    public class AdminClientBuilder
    {
        internal IEnumerable<KeyValuePair<string, string>> config;
        internal Action<AdminClient, Error> errorHandler;
        internal Action<AdminClient, LogMessage> logHandler;
        internal Action<AdminClient, string> statsHandler;

        /// <summary>
        ///     Initialize a new <see cref="AdminClientBuilder" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' and must be specified.
        /// </param>
        public AdminClientBuilder(IEnumerable<KeyValuePair<string, string>> config)
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
        ///     Executes on the poll thread (a background thread managed by
        ///     the admin client).
        /// </remarks>
        public AdminClientBuilder SetStatisticsHandler(Action<AdminClient, string> statisticsHandler)
        {
            this.statsHandler = statisticsHandler;
            return this;
        }

        /// <summary>
        ///     Set the handler to call on error events e.g. connection failures or all
        ///     brokers down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal - such errors should be interpreted
        ///     as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     Executes on the poll thread (a background thread managed by the admin
        ///     client).
        /// </remarks>
        public AdminClientBuilder SetErrorHandler(Action<AdminClient, Error> errorHandler)
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
        public AdminClientBuilder SetLogHandler(Action<AdminClient, LogMessage> logHandler)
        {
            this.logHandler = logHandler;
            return this;
        }

        /// <summary>
        ///     Build the <see cref="AdminClient" /> instance.
        /// </summary>
        public AdminClient Build()
        {
            return new AdminClient(this);
        }
    }
}
