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
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Expose administrative commands to interact with broker state and configuration.
    /// </summary>
    public interface IAdmin
    {
        /// <summary>
        ///     Delete a set of topics and all its replica.
        ///     This methold will block fot ar most timeout.
        /// </summary>
        /// <param name="topics">
        ///     The list of topics to delete.
        /// </param>
        /// <param name="timeout">
        ///     The maximum time the method will block, must be positive or zero.
        ///     It serves as timeout for controller lookup (try to send request
        ///     to broker) and remaining time will serve as timeout for request 
        ///     if <paramref name="waitForDeletion"/> is true.
        /// </param>
        /// <param name="waitForDeletion">
        ///     If false, the method return as soon as broker receive the 
        ///     request, and <see cref="ErrorCode.NoError"/> denotes topic
        ///     was marked for deletion.
        ///
        ///     If true, the broker will wait for at most remaining timeout for
        ///     deletion to complete. Return may then be populated with
        ///     <see cref="ErrorCode.RequestTimedOut"/> if completion did
        ///     not complete in remaining time (topic will still be marked
        ///     for deletion)
        /// </param>
        /// <returns>
        ///     A list corresponding to topics asked for deletion with a 
        ///     corresponding error.
        ///     
        ///     Any other error denote the deletetopic request failed for the 
        ///     given topic (or timed out if <paramref name="waitForDeletion"/> is enable
        /// </returns>
        /// <exception cref="KafkaException">
        ///     Exception indicating the delete topic request encountered an error.
        ///     This may either denote a fail in sending request to broker,
        ///     its treatment, or if a common error occured on all topics.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
        List<TopicError> DeleteTopics(IEnumerable<string> topics, TimeSpan timeout, bool waitForDeletion);
    }
}
