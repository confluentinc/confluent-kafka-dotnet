// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Metadata pertaining to a single Kafka broker.
    /// </summary>
    public class BrokerMetadata
    {
        /// <summary>
        ///     Initializes a new BrokerMetadata class instance.
        /// </summary>
        /// <param name="brokerId">
        ///     The Kafka broker id.
        /// </param>
        /// <param name="host">
        ///     The Kafka broker hostname.
        /// </param>
        /// <param name="port">
        ///     The Kafka broker port.
        /// </param>
        public BrokerMetadata(int brokerId, string host, int port)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }

        /// <summary>
        ///     Gets the Kafka broker id.
        /// </summary>
        public int BrokerId { get; }

        /// <summary>
        ///     Gets the Kafka broker hostname.
        /// </summary>
        public string Host { get; }

        /// <summary>
        ///     Gets the Kafka broker port.
        /// </summary>
        public int Port { get; }

        /// <summary>
        ///     Returns a JSON representation of the BrokerMetadata object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of the BrokerMetadata object.
        /// </returns>
        public override string ToString()
            => $"{{ \"BrokerId\": {BrokerId}, \"Host\": \"{Host}\", \"Port\": {Port} }}";
    }
}
