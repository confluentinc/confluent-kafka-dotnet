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
    ///     A builder for <see cref="IAdminClient" />.
    /// </summary>
    public class AdminClientBuilder
    {
        /// <summary>
        ///     The config dictionary.
        /// </summary>
        internal protected IEnumerable<KeyValuePair<string, string>> Config { get; set; }

        /// <summary>
        ///     The configured error handler.
        /// </summary>
        internal protected Action<IAdminClient, Error> ErrorHandler { get; set; }
        
        /// <summary>
        ///     The configured log handler.
        /// </summary>
        internal protected Action<IAdminClient, LogMessage> LogHandler { get; set; }

        /// <summary>
        ///     The configured statistics handler.
        /// </summary>
        internal protected Action<IAdminClient, string> StatisticsHandler { get; set; }

        /// <summary>
        ///     The configured OAuthBearer Token Refresh handler.
        /// </summary>
        public Action<IProducer<Null, Null>, string> OAuthBearerTokenRefreshHandler { get; set; }

        /// <summary>
        ///     Initialize a new <see cref="AdminClientBuilder" /> instance.
        /// </summary>
        /// <param name="config">
        ///     A collection of librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        ///     and parameters specific to this client (refer to: 
        ///     <see cref="Confluent.Kafka.ConfigPropertyNames" />).
        ///     At a minimum, 'bootstrap.servers' must be specified.
        /// </param>
        public AdminClientBuilder(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.Config = config;
        }

        /// <summary>
        ///     Set the handler to call on statistics events. Statistics are provided
        ///     as a JSON formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
        /// </summary>
        /// <remarks>
        ///     You can enable statistics by setting the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     Executes on the poll thread (a background thread managed by
        ///     the admin client).
        /// </remarks>
        public AdminClientBuilder SetStatisticsHandler(Action<IAdminClient, string> statisticsHandler)
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
        ///     Executes on the poll thread (a background thread managed by the admin
        ///     client).
        /// </remarks>
        public AdminClientBuilder SetErrorHandler(Action<IAdminClient, Error> errorHandler)
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
        public AdminClientBuilder SetLogHandler(Action<IAdminClient, LogMessage> logHandler)
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
        ///     is triggered via <see cref="IAdminClient"/>'s admin methods
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
        ///     IProducer - instance of the admin client's inner producer instance
        ///     which should be used to set token or token failure string - Value of configuration
        ///     property sasl.oauthbearer.config
        /// </param>
        public AdminClientBuilder SetOAuthBearerTokenRefreshHandler(Action<IProducer<Null, Null>, string> oAuthBearerTokenRefreshHandler)
        {
            if (this.OAuthBearerTokenRefreshHandler != null)
            {
                throw new InvalidOperationException("OAuthBearer token refresh handler may not be specified more than once.");
            }
            this.OAuthBearerTokenRefreshHandler = oAuthBearerTokenRefreshHandler;
            return this;
        }

        /// <summary>
        ///     Build the <see cref="AdminClient" /> instance.
        /// </summary>
        public virtual IAdminClient Build()
        {
            return new AdminClient(this);
        }
    }
}
