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
    ///     Represents an error that occurred during a create ACLs request.
    /// </summary>
    public class CreateAclsException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of CreateAclsException.
        /// </summary>
        /// <param name="results">
        ///     The result corresponding to all the ACLs in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </param>
        public CreateAclsException(List<CreateAclReport> results)
            : base(new Error(ErrorCode.Local_Partial,
                "An error occurred creating ACLs: [" +
                String.Join(", ", results.Select(r => r.Error)) +
                "]."))
        {
            this.Results = results;
        }

        /// <summary>
        ///     The result corresponding to all the ACLs in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </summary>
        public List<CreateAclReport> Results { get; }

        /// <summary>
        ///     Tests whether this CreateAclsException instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a CreateAclsException and the <see cref="KafkaException.Error"/> and <see cref="Results"/> property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var exception = (CreateAclsException) obj;
            if (base.Equals(exception)) return true;
            return this.Error == exception.Error &&
                (this.Results?.SequenceEqual(exception.Results) ?? exception.Results == null);
        }

        /// <summary>
        ///     Tests whether CreateAclsException instance a is equal to CreateAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first CreateAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second CreateAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if CreateAclsException instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(CreateAclsException a, CreateAclsException b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether CreateAclsException instance a is not equal to CreateAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first CreateAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second CreateAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if CreateAclsException instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(CreateAclsException a, CreateAclsException b)
            => !(a == b);

        /// <summary>
        ///     Returns a hash code for this CreateAclsException value.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this CreateAclsException value.
        /// </returns>
        public override int GetHashCode()
        {
            int hash = 1;
            if (Error != null) hash ^= Error.GetHashCode();
            if (Results != null)
            {
                foreach(CreateAclReport result in Results)
                {
                    hash ^= result.GetHashCode();
                }
            }
            return hash;
        }
    }
}
