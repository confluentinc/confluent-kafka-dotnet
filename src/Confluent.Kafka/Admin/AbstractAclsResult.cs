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
using System.Collections.Generic;
using System.Linq;

namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///    Base class for ACL results with an Error and a list of AclBindings
    /// </summary>
    public abstract class AbstractAclsResult
    {
        /// <summary>
        ///     List of ACL bindings in this result
        /// </summary>
        public List<AclBinding> AclBindings { get; set; }

        /// <summary>
        ///     Operation error status, null if successful.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is of the same type as obj and the Error and AclBindings property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var result = (AbstractAclsResult) obj;
            if (base.Equals(result)) return true;
            return this.Error == result.Error &&
                (this.AclBindings == null ? result.AclBindings == null :
                new HashSet<AclBinding>(this.AclBindings).SetEquals(new HashSet<AclBinding>(result.AclBindings)));
        }

        /// <summary>
        ///     Tests whether AbstractAclsResult instance a is equal to AbstractAclsResult instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AbstractAclsResult instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AbstractAclsResult instance to compare.
        /// </param>
        /// <returns>
        ///     true if AbstractAclsResult instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(AbstractAclsResult a, AbstractAclsResult b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether AbstractAclsResult instance a is not equal to AbstractAclsResult instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AbstractAclsResult instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AbstractAclsResult instance to compare.
        /// </param>
        /// <returns>
        ///     true if AbstractAclsResult instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(AbstractAclsResult a, AbstractAclsResult b)
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
            if (AclBindings != null)
            {
                foreach(AclBinding aclBinding in AclBindings)
                {
                    hash ^= aclBinding.GetHashCode();
                }
            }
            return hash;
        }
    }
}
