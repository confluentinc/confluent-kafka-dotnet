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
    ///     Defines methods common to all client types.
    /// </summary>
    public interface IClient : IDisposable
    {
        /// <summary>
        ///     An opaque reference to the underlying
        ///     librdkafka client instance. This can be used
        ///     to construct an AdminClient that utilizes the
        ///     same underlying librdkafka client as this
        ///     instance.
        /// </summary>
        Handle Handle { get; }


        /// <summary>
        ///     Gets the name of this client instance.
        ///
        ///     Contains (but is not equal to) the client.id
        ///     configuration parameter.
        /// </summary>
        /// <remarks>
        ///     This name will be unique across all client
        ///     instances in a given application which allows
        ///     log messages to be associated with the
        ///     corresponding instance.
        /// </remarks>
        string Name { get; }


        /// <summary>
        ///     Adds one or more brokers to the Client's list
        ///     of initial bootstrap brokers. 
        ///
        ///     Note: Additional brokers are discovered
        ///     automatically as soon as the Client connects
        ///     to any broker by querying the broker metadata.
        ///     Calling this method is only required in some
        ///     scenarios where the address of all brokers in
        ///     the cluster changes.
        /// </summary>
        /// <param name="brokers">
        ///     Comma-separated list of brokers in
        ///     the same format as the bootstrap.server
        ///     configuration parameter.
        /// </param>
        /// <remarks>
        ///     There is currently no API to remove existing
        ///     configured, added or learnt brokers.
        /// </remarks>
        /// <returns>
        ///     The number of brokers added. This value
        ///     includes brokers that may have been specified
        ///     a second time.
        /// </returns>
        int AddBrokers(string brokers);

        /// <summary>
        ///     SetSaslCredentials sets the SASL credentials used for this
        ///     client.
        ///     The new credentials will overwrite the old ones (which were set
        ///     when creating client or by a previous call to
        ///     SetSaslCredentials). The new credentials will be used the next
        ///     time the client needs to authenticate to a broker.
        ///     This method will not disconnect existing broker connections that
        ///     were established with the old credentials.
        ///     This method applies only to the SASL PLAIN and SCRAM mechanisms.
        /// </summary>
        /// <param name="username">
        ///     The username to set.
        /// </param>
        /// <param name="password">
        ///     The password to set.
        /// </param>
        void SetSaslCredentials(string username, string password);
    }
}
