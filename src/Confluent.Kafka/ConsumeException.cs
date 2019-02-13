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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents an error that occured during message consumption.
    /// </summary>
    public class ConsumeException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of ConsumeException
        /// </summary>
        /// <param name="consumerRecord">
        ///     An object that provides information know about the consumer 
        ///     record for which the error occured.
        /// </param>
        /// <param name="error">
        ///     The error that occured.
        /// </param>
        /// <param name="innerException">
        ///     The exception instance that caused this exception.
        /// </param>
        public ConsumeException(ConsumeResult<byte[], byte[]> consumerRecord, Error error, Exception innerException)
            : base(error, innerException)
        {
            ConsumerRecord = consumerRecord;
        }

        /// <summary>
        ///     Initialize a new instance of ConsumeException
        /// </summary>
        /// <param name="consumerRecord">
        ///     An object that provides information know about the consumer 
        ///     record for which the error occured.
        /// </param>
        /// <param name="error">
        ///     The error that occured.
        /// </param>
        public ConsumeException(ConsumeResult<byte[], byte[]> consumerRecord, Error error)
            : base(error)
        {
            ConsumerRecord = consumerRecord;
        }

        /// <summary>
        ///     An object that provides information known about the consumer
        ///     record for which the error occured.
        /// </summary>
        public ConsumeResult<byte[], byte[]> ConsumerRecord { get; private set; }
    }
}
