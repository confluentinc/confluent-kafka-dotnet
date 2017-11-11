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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a Kafka (topic, error) tuple.
    /// </summary>
    public class TopicError
    {
        /// <summary>
        ///     Initializes a new TopicError instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        public TopicError(string topic, Error error)
        {
            Topic = topic;
            Error = error;
        }

        /// <summary>
        ///     Gets the Kafka topic name.
        /// </summary>
        public string Topic { get; }
        
        /// <summary>
        ///     Gets the Kafka error.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Tests whether this TopicError instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicError and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicError tp))
            {
                return false;
            }
            
            return tp.Topic == Topic && tp.Error == Error;
        }

        /// <summary>
        ///     Returns a hash code for this TopicError.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicError.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => (Topic.GetHashCode())*251 + Error.GetHashCode();

        /// <summary>
        ///     Tests whether TopicError instance a is equal to TopicError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicError instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicError a, TopicError b)
        {
            if (object.ReferenceEquals(a, null))
            {
                return object.ReferenceEquals(b, null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicError instance a is not equal to TopicError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicError instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicError a, TopicError b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicError object.
        /// </summary>
        /// <returns>
        ///     A string representation of the TopicError object.
        /// </returns>
        public override string ToString()
            => $"{Topic} : {Error}";
    }
}
