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

using System;


namespace Confluent.Kafka
{
    /// <summary>
    ///     This interface is implemented by types that handle delivery report
    ///     callbacks as a result of calls to Confluent.Kafka.Producer.ProduceAsync().
    /// </summary>
    /// <remarks>
    ///     Methods of this interface will be executed on the poll thread and will
    ///     block other operations - consider this when implementing.
    /// </remarks>
    public interface IDeliveryHandler
    {
        /// <summary>
        ///     Gets whether or not to marshal key and value data 
        ///     from librdkafka when the delivery report is 
        ///     available. Usually this should return true.
        ///     Return false for a small improvement in throughput
        ///     if you don't need this information.
        /// </summary>
        bool MarshalData { get; }

        /// <summary>
        ///     Gets whether or not to marshal message headers.
        ///     Set this to false for a increased throughput if
        ///     you don't need this information.
        /// </summary>
        bool MarshalHeaders { get; }
        
        /// <summary>
        ///     This method is called when the delivery report
        ///     is available
        /// </summary>
        /// <param name="message">
        ///     The delivery report.
        /// </param>
        void HandleDeliveryReport(Message message);
    }

    /// <summary>
    ///     This interface is implemented by types that handle delivery report
    ///     callbacks as a result of calls to Confluent.Kafka.Producer&lt;TKey,TValue&gt;.ProduceAsync().
    /// </summary>
    /// <remarks>
    ///     Methods of this interface will be executed on the poll thread and will
    ///     block other operations - consider this when implementing.
    /// </remarks>
    public interface IDeliveryHandler<TKey, TValue>
    {
        /// <summary>
        ///     Gets whether or not to marshal key and value data 
        ///     from librdkafka when the delivery report is 
        ///     available. Usually this should return true.
        ///     Return false for a small improvement in throughput
        ///     if you don't need this information.
        /// </summary>
        bool MarshalData { get; }

        /// <summary>
        ///     Gets whether or not to marshal message headers.
        ///     Set this to false for a increased throughput if
        ///     you don't need this information.
        /// </summary>
        bool MarshalHeaders { get; }

        /// <summary>
        ///     This method is called when the delivery report
        ///     is available
        /// </summary>
        /// <param name="message">
        ///     The delivery report.
        /// </param>
        void HandleDeliveryReport(Message<TKey, TValue> message);
    }

}
