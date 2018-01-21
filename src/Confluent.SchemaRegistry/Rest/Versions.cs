// Copyright 2016-2018 Confluent Inc.
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


namespace  Confluent.SchemaRegistry
{
    internal static class Versions
    {
        public const string SchemaRegistry_V1_JSON = "application/vnd.schemaregistry.v1+json";
        public const string SchemaRegistry_Default_JSON = "application/vnd.schemaregistry+json";
        public const string JSON = "application/json";
        
        public static readonly IReadOnlyList<string> PreferredResponseTypes = new List<string> 
        { 
            SchemaRegistry_V1_JSON, 
            SchemaRegistry_Default_JSON, 
            JSON 
        };

        /// <remarks>
        ///     This type is completely generic and carries no actual information about the type of data, but
        ///     it is the default for request entities if no content type is specified. Well behaving users
        ///     of the API will always specify the content type, but ad hoc use may omit it. We treat this as
        ///     JSON since that's all we currently support.
        /// </remarks>
        public const string GenericRequest = "application/octet-stream";
    }
}
