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
using System.Collections.Generic;
using Newtonsoft.Json;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Response from creating or updating associations.
    /// </summary>
    public class AssociationResponse
    {
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
        ///     The associations that were created or updated.
        /// </summary>
        [JsonProperty("associations")]
        public List<AssociationInfo> Associations { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="AssociationResponse"/> class.
        /// </summary>
        public AssociationResponse()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="AssociationResponse"/> class.
        /// </summary>
        public AssociationResponse(
            string resourceName,
            string resourceNamespace,
            string resourceId,
            string resourceType,
            List<AssociationInfo> associations)
        {
            ResourceName = resourceName;
            ResourceNamespace = resourceNamespace;
            ResourceId = resourceId;
            ResourceType = resourceType;
            Associations = associations;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (obj is not AssociationResponse other)
            {
                return false;
            }

            return ResourceName == other.ResourceName &&
                   ResourceNamespace == other.ResourceNamespace &&
                   ResourceId == other.ResourceId &&
                   ResourceType == other.ResourceType &&
                   Equals(Associations, other.Associations);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(ResourceName, ResourceNamespace, ResourceId, ResourceType, Associations);
        }
    }
}
