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
    ///     Defines methods common to all Apache Kafka client types.
    /// </summary>
    public interface IClient : IDisposable
    {
        /// <include file='include_docs_client.xml' path='API/Member[@name="Handle"]/*' />
        Handle Handle { get; }

        /// <include file='include_docs_client.xml' path='API/Member[@name="Name"]/*' />
        string Name { get; }

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        int AddBrokers(string brokers);

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        event EventHandler<LogMessage> OnLog;

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        event EventHandler<string> OnStatistics;

        /// <include file='include_docs_producer.xml' path='API/Member[@name="OnError"]/*' />
        event EventHandler<Error> OnError;
    }
}
