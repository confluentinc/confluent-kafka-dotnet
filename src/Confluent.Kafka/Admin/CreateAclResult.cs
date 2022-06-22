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

namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Provides create ACL error information.
    /// </summary>
    public class CreateAclResult
    {
        /// <summary>
        ///     Per ACL binding error status.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     Tests whether this CreateAclResult instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a CreateAclResult and the <see cref="Error"/> property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var result = (CreateAclResult) obj;
            if (base.Equals(result)) return true;
            return this.Error == result.Error;
        }

        /// <summary>
        ///     Tests whether CreateAclResult instance a is equal to CreateAclResult instance b.
        /// </summary>
        /// <param name="a">
        ///     The first CreateAclResult instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second CreateAclResult instance to compare.
        /// </param>
        /// <returns>
        ///     true if CreateAclResult instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(CreateAclResult a, CreateAclResult b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether CreateAclResult instance a is not equal to CreateAclResult instance b.
        /// </summary>
        /// <param name="a">
        ///     The first CreateAclResult instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second CreateAclResult instance to compare.
        /// </param>
        /// <returns>
        ///     true if CreateAclResult instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(CreateAclResult a, CreateAclResult b)
            => !(a == b);

        /// <summary>
        ///     Returns a hash code for this CreateAclResult value.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this CreateAclResult value.
        /// </returns>
        public override int GetHashCode()
        {
            if (Error == null) return 0;
            return Error.GetHashCode();
        }
    }
}
