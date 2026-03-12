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
    ///     Represents an association between a subject and a resource in Schema Registry.
    /// </summary>
    public class Association
    {
        /// <summary>
        ///     The subject name.
        /// </summary>
        [JsonProperty("subject")]
        public string Subject { get; set; }

        /// <summary>
        ///     The globally unique identifier.
        /// </summary>
        [JsonProperty("guid")]
        public string Guid { get; set; }

        /// <summary>
        ///     The resource name (e.g., topic name).
        /// </summary>
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        /// <summary>
        ///     The resource namespace (e.g., Kafka cluster ID).
        /// </summary>
        [JsonProperty("resourceNamespace")]
        public string ResourceNamespace { get; set; }

        /// <summary>
        ///     The resource identifier.
        /// </summary>
        [JsonProperty("resourceId")]
        public string ResourceId { get; set; }

        /// <summary>
        ///     The type of resource (e.g., "topic").
        /// </summary>
        [JsonProperty("resourceType")]
        public string ResourceType { get; set; }

        /// <summary>
        ///     The type of association (e.g., "key" or "value").
        /// </summary>
        [JsonProperty("associationType")]
        public string AssociationType { get; set; }

        /// <summary>
        ///     The lifecycle policy of the association.
        /// </summary>
        [JsonProperty("lifecycle")]
        public string Lifecycle { get; set; }

        /// <summary>
        ///     Whether the association is frozen.
        /// </summary>
        [JsonProperty("frozen")]
        public bool Frozen { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Association"/> class.
        /// </summary>
        public Association()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Association"/> class.
        /// </summary>
        public Association(
            string subject,
            string guid,
            string resourceName,
            string resourceNamespace,
            string resourceId,
            string resourceType,
            string associationType,
            string lifecycle,
            bool frozen)
        {
            Subject = subject;
            Guid = guid;
            ResourceName = resourceName;
            ResourceNamespace = resourceNamespace;
            ResourceId = resourceId;
            ResourceType = resourceType;
            AssociationType = associationType;
            Lifecycle = lifecycle;
            Frozen = frozen;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (obj is not Association other)
            {
                return false;
            }

            return Subject == other.Subject &&
                   Guid == other.Guid &&
                   ResourceName == other.ResourceName &&
                   ResourceNamespace == other.ResourceNamespace &&
                   ResourceId == other.ResourceId &&
                   ResourceType == other.ResourceType &&
                   AssociationType == other.AssociationType &&
                   Lifecycle == other.Lifecycle &&
                   Frozen == other.Frozen;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(
                HashCode.Combine(Subject, Guid, ResourceName, ResourceNamespace),
                HashCode.Combine(ResourceId, ResourceType, AssociationType, Lifecycle),
                Frozen);
        }
    }
}
