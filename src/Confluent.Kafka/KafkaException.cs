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
    ///     Represents an error that occured during an interaction with Kafka.
    /// </summary>
    public class KafkaException : Exception
    {
        /// <summary>
        ///     Initialize a new instance of KafkaException based on 
        ///     an existing Error instance.
        /// </summary>
        /// <param name="error">
        ///     The Kafka Error.
        /// </param>
        public KafkaException(Error error)
            : base(error.ToString())
        {
            Error = error;
        }

        /// <summary>
        ///     Initialize a new instance of KafkaException based on
        ///     an existing Error instance and inner exception.
        /// </summary>
        /// <param name="error">
        ///     The Kafka Error.
        /// </param>
        /// <param name="innerException">
        ///     The exception instance that caused this exception.
        /// </param>
        public KafkaException(Error error, Exception innerException)
            : base(error.Reason, innerException)
        {
            Error = error;            
        }

        /// <summary>
        ///     Initialize a new instance of KafkaException based on 
        ///     an existing ErrorCode value.
        /// </summary>
        /// <param name="code"> 
        ///     The Kafka ErrorCode.
        /// </param>
        public KafkaException(ErrorCode code)
            : base(ErrorCodeExtensions.GetReason(code))
        {
            Error = new Error(code);
        }

        /// <summary>
        ///     Gets the Error associated with this KafkaException.
        /// </summary>
        public Error Error { get; }
    }
}
