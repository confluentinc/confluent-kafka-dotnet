// Copyright 2016-2017 Confluent Inc.
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

using System.Collections.Generic;


namespace  Confluent.Kafka.SchemaRegistry.Rest
{
    internal static class Versions
    {
        public const string SCHEMA_REGISTRY_V1_JSON = "application/vnd.schemaregistry.v1+json";
        // Default weight = 1
        public const string SCHEMA_REGISTRY_V1_JSON_WEIGHTED = SCHEMA_REGISTRY_V1_JSON;
        // These are defaults that track the most recent API version. These should always be specified
        // anywhere the latest version is produced/consumed.
        public const string SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = SCHEMA_REGISTRY_V1_JSON;
        public const string SCHEMA_REGISTRY_DEFAULT_JSON = "application/vnd.schemaregistry+json";
        public const string SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED =
            SCHEMA_REGISTRY_DEFAULT_JSON +
            "; qs=0.9";
        public const string JSON = "application/json";
        public const string JSON_WEIGHTED = JSON + "; qs=0.5";

        public static readonly IReadOnlyList<string> PREFERRED_RESPONSE_TYPES = new List<string> { SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON };

        // This type is completely generic and carries no actual information about the type of data, but
        // it is the default for request entities if no content type is specified. Well behaving users
        // of the API will always specify the content type, but ad hoc use may omit it. We treat this as
        // JSON since that's all we currently support.
        public const string GENERIC_REQUEST = "application/octet-stream";
    }
}
