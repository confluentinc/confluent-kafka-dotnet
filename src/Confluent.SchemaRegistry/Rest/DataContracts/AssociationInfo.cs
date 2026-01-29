// Copyright 2025 Confluent Inc.
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
using Newtonsoft.Json;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Information about an association in a response.
    /// </summary>
    public class AssociationInfo
    {
        /// <summary>
        ///     The subject name.
        /// </summary>
        [JsonProperty("subject")]
        public string Subject { get; set; }

        /// <summary>
        ///     The type of association (e.g., "key" or "value").
        /// </summary>
        [JsonProperty("associationType")]
        public string AssociationType { get; set; }

        /// <summary>
        ///     The lifecycle policy.
        /// </summary>
        [JsonProperty("lifecycle")]
        public string Lifecycle { get; set; }

        /// <summary>
        ///     Whether the association is frozen.
        /// </summary>
        [JsonProperty("frozen")]
        public bool Frozen { get; set; }

        /// <summary>
        ///     The schema associated with this association.
        /// </summary>
        [JsonProperty("schema")]
        public Schema Schema { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="AssociationInfo"/> class.
        /// </summary>
        public AssociationInfo()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="AssociationInfo"/> class.
        /// </summary>
        public AssociationInfo(
            string subject,
            string associationType,
            string lifecycle,
            bool frozen,
            Schema schema)
        {
            Subject = subject;
            AssociationType = associationType;
            Lifecycle = lifecycle;
            Frozen = frozen;
            Schema = schema;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (obj is not AssociationInfo other)
            {
                return false;
            }

            return Subject == other.Subject &&
                   AssociationType == other.AssociationType &&
                   Lifecycle == other.Lifecycle &&
                   Frozen == other.Frozen &&
                   Equals(Schema, other.Schema);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(Subject, AssociationType, Lifecycle, Frozen, Schema);
        }
    }
}
