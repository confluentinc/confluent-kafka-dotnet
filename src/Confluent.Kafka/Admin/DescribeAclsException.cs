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
    ///     Represents an error that occurred during a describe ACLs request.
    /// </summary>
    public class DescribeAclsException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of DescribeAclsException.
        /// </summary>
        /// <param name="result">
        ///     The result corresponding to the ACL filter in the request
        /// </param>
        public DescribeAclsException(DescribeAclsReport result)
            : base(new Error(ErrorCode.Local_Partial,
                $"An error occurred describing ACLs: {result}."))
        {
            Result = result;
        }

        /// <summary>
        ///     The result corresponding to the describe ACLs operation in the request 
        /// </summary>
        public DescribeAclsReport Result { get; }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is of the same type as obj and the <see cref="KafkaException.Error"/> and <see cref="Result"/> property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var exception = (DescribeAclsException) obj;
            if (base.Equals(exception)) return true;
            return Error == exception.Error &&
                   Result == exception.Result;
        }


        /// <summary>
        ///     Tests whether DescribeAclsException instance a is equal to DescribeAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first DescribeAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second DescribeAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if DescribeAclsException instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(DescribeAclsException a, DescribeAclsException b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether DescribeAclsException instance a is not equal to DescribeAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first DescribeAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second DescribeAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if DescribeAclsException instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(DescribeAclsException a, DescribeAclsException b)
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
            if (Result != null)
            {
                hash ^= Result.GetHashCode();
            }
            return hash;
        }
    }
}
