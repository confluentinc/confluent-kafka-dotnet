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
    ///     Encapsulates a librdkafka error and indication of whether or
    ///     not the error should be considered fatal.
    /// </summary>
    public class ErrorEvent : Error
    {
        /// <summary>
        ///     Initialize a new ErrorEvent instance 
        /// </summary>
        /// <param name="error">
        ///     The error that occured.
        /// </param>
        /// <param name="isFatal">
        ///     whether or not the error is fatal.
        /// </param>
        public ErrorEvent(Error error, bool isFatal)
            : base(error)
        {
            IsFatal = isFatal;
        }

        /// <summary>
        ///     Whether or not the event is fatal.
        /// </summary>
        public bool IsFatal { get; set; }
    }
}
