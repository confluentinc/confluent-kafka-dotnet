// Copyright 2022 Confluent Inc.
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
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents an error that occurred during an ACLs request.
    /// </summary>
    public abstract class AbstractAclsException<T> : KafkaException where T: AbstractAclsResult
    {
        /// <summary>
        ///     Initialize a new instance of AbstractAclsException.
        /// </summary>
        /// <param name="message">
        ///     The message to show in the Error.
        /// </param>
        /// <param name="results">
        ///     The result corresponding to all the ACL operations in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </param>
        public AbstractAclsException(string message, List<T> results)
            : base(new Error(ErrorCode.Local_Partial,
                $"{message}: [{String.Join(", ", results.Select(r => r.ToString()))}]."))
        {
            this.Results = results;
        }

        /// <summary>
        ///     The result corresponding to all the ACL operations in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </summary>
        public List<T> Results { get; }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is of the same type as obj and the Error and Results property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var exception = (AbstractAclsException<T>) obj;
            if (base.Equals(exception)) return true;
            return this.Error == exception.Error &&
                (this.Results?.SequenceEqual(exception.Results) ?? exception.Results == null);
        }


        /// <summary>
        ///     Tests whether AbstractAclsException instance a is equal to AbstractAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AbstractAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AbstractAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if AbstractAclsException instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(AbstractAclsException<T> a, AbstractAclsException<T> b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether AbstractAclsException instance a is not equal to AbstractAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AbstractAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AbstractAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if AbstractAclsException instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(AbstractAclsException<T> a, AbstractAclsException<T> b)
            => !(a == b);

        /// <summary>
        ///     Returns a hash code for this value.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this value.
        /// </returns>
        public override int GetHashCode()
        {
            int hash = 1;
            if (Error != null) hash ^= Error.GetHashCode();
            if (Results != null)
            {
                foreach(T result in Results)
                {
                    hash ^= result.GetHashCode();
                }
            }
            return hash;
        }
    }
}
