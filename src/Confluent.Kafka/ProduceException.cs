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
    ///     Represents an error that occured whilst producing a message.
    /// </summary>
    public class ProduceException<TKey, TValue> : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of ProduceException based on 
        ///     an existing Error value.
        /// </summary>
        /// <param name="error"> 
        ///     The error associated with the delivery report.
        /// </param>
        /// <param name="deliveryReport">
        ///     The delivery report associated with the produce request.
        /// </param>
        public ProduceException(Error error, DeliveryReport<TKey, TValue> deliveryReport)
            : base(error)
        {
            DeliveryReport = deliveryReport;
        }

        /// <summary>
        ///     The delivery report associated with the produce request.
        /// </summary>
        public DeliveryReport<TKey, TValue> DeliveryReport { get; }
    }
}
