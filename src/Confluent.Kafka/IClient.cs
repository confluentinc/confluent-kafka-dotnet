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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines methods common to all client types.
    /// </summary>
    public interface IClient : IDisposable
    {
        /// <summary>
        ///     An opaque reference to the underlying librdkafka client instance.
        ///     This can be used to construct an AdminClient that utilizes the same
        ///     underlying librdkafka client as this instance.
        /// </summary>
        Handle Handle { get; }


        /// <summary>
        ///     Gets the name of this client instance.
        ///     Contains (but is not equal to) the client.id configuration
        ///     parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all client instances
        ///     in a given application which allows log messages to be
        ///     associated with the corresponding instance.
        /// </remarks>
        string Name { get; }


        /// <summary>
        ///     Adds one or more brokers to the Client's list of initial
        ///     bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered automatically as
        ///     soon as the Client connects to any broker by querying the
        ///     broker metadata. Calling this method is only required in
        ///     some scenarios where the address of all brokers in the
        ///     cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Comma-separated list of brokers in the same format as 
        ///     the bootstrap.server configuration parameter.
        /// </param>
        /// <remarks>
        ///     There is currently no API to remove existing configured, 
        ///     added or learnt brokers.
        /// </remarks>
        /// <returns>
        ///     The number of brokers added. This value includes brokers
        ///     that may have been specified a second time.
        /// </returns>
        int AddBrokers(string brokers);


        /// <summary>
        ///     Raised when there is information that should be logged.
        ///     If not specified, a default callback that writes to stderr
        ///     will be used.
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
        event EventHandler<LogMessage> OnLog;


        /// <summary>
        ///     Raised on error events e.g. connection failures or all brokers
        ///     down. Note that the client will try to automatically recover from
        ///     errors that are not marked as fatal - these errors should be
        ///     seen as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     On the Consumer, executes as a side-effect of
        ///     <see cref=""Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)"" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        event EventHandler<ErrorEvent> OnError;

        /// <summary>
        ///     Raised on librdkafka statistics events - a JSON
        ///     formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     On the Consumer, executes as a side-effect of
        ///     <see cref=""Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)"" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        event EventHandler<string> OnStatistics;
    }
}
