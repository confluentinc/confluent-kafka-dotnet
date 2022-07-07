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
    ///     Represents an error that occurred during a delete ACLs request.
    /// </summary>
    public class DeleteAclsException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of DeleteAclsException.
        /// </summary>
        /// <param name="reports">
        ///     The result corresponding to all the ACL filters in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </param>
        public DeleteAclsException(List<DeleteAclsReport> reports)
            : base(new Error(ErrorCode.Local_Partial,
                $"An error occurred deleting ACLs: [{string.Join(", ", reports.Select(r => r.ToString()))}]."))
        {
            Reports = reports;
        }

        /// <summary>
        ///     The result corresponding to all the delete ACLs operations in the request 
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </summary>
        public List<DeleteAclsReport> Reports { get; }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is of the same type as obj and the <see cref="KafkaException.Error"/> and <see cref="Reports"/> property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            var exception = (DeleteAclsException) obj;
            if (base.Equals(exception)) return true;
            return Error == exception.Error &&
                (Reports?.SequenceEqual(exception.Reports) ?? exception.Reports == null);
        }


        /// <summary>
        ///     Tests whether DeleteAclsException instance a is equal to DeleteAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first DeleteAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second DeleteAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if DeleteAclsException instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(DeleteAclsException a, DeleteAclsException b)
        {
            if (a is null)
            {
                return b is null;
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether DeleteAclsException instance a is not equal to DeleteAclsException instance b.
        /// </summary>
        /// <param name="a">
        ///     The first DeleteAclsException instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second DeleteAclsException instance to compare.
        /// </param>
        /// <returns>
        ///     true if DeleteAclsException instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(DeleteAclsException a, DeleteAclsException b)
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
            if (Reports != null)
            {
                foreach(DeleteAclsReport report in Reports)
                {
                    hash ^= report.GetHashCode();
                }
            }
            return hash;
        }
    }
}
